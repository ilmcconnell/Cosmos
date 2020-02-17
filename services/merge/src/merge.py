"""
Extract objects
"""
import pandas as pd
import json
import pymongo
from pymongo import MongoClient
import os
import logging as l
import logstash
l.basicConfig(format='%(levelname)s :: %(asctime)s :: %(message)s', level=l.DEBUG)
logging = l.getLogger()
logging.addHandler(logstash.TCPLogstashHandler('services_logstash_1', 5000))
import time
import io
from PIL import Image, ImageStat
import re
from joblib import Parallel, delayed
import click
import pickle
from bson import ObjectId
from group_cls import group_cls


def load_pages(db, buffer_size):
    """
    """
    current_docs = []
#    for doc in db.propose_pages.find({'postprocess': True, "$or" : [{'merged': False}, {"merged" : {"$exists" : False}}]}, no_cursor_timeout=True):
    for doc in db.propose_pages.find({'_id': ObjectId("5e2f6ebbd523ce570a4bb4da")}, no_cursor_timeout=True):
        current_docs.append(doc)
        if len(current_docs) == buffer_size:
            yield current_docs
            current_docs = []
    yield current_docs


def merge_objs(page):
    if 'pp_detected_objs' not in page:
        return (None, f'This page has not had postprocessing done on it')
    if page['pp_detected_objs'] is None or len(page['pp_detected_objs']) == 0:
        return (None, f'No detected objs on page: {page["_id"]}')
    detected_objs = page['pp_detected_objs']
    # Sanity check that filters objects not of length 3
    detected_objs = [obj for obj in detected_objs if len(obj) == 3]
    merged_objs = group_cls(detected_objs, 'Table', do_table_merge=True, merge_over_classes=['Figure', 'Section Header', 'Page Footer', 'Page Header'])
    merged_objs = group_cls(merged_objs, 'Figure')
    page['merged_objs'] = merged_objs
    logging.info(f"{len(detected_objs)} objects merged into {len(merged_objs)}")
    return page


def merge_scan(db_insert_fn, num_processes):
    logging.info('Starting object extraction over pages')
    start_time = time.time()
    client = MongoClient(os.environ['DBCONNECT'])
    logging.info(f'Connected to client: {client}')
    db = client.pdfs
    n = 0
    for batch in load_pages(db, num_processes):
        n+=len(batch)
        pages = Parallel(n_jobs=num_processes)(delayed(merge_objs)(page) for page in batch)
        db_insert_fn(pages, client)
    end_time = time.time()
    logging.info(f'End merging. Total time: {end_time - start_time} s ({n} pages, {num_processes} threads)')

def mongo_insert_fn(pages, client):
    db = client.pdfs
    for page in pages:
        try:
            result = db.propose_pages.update_one({'_id': page['_id']},
                                             {'$set':
                                                {
                                                    'merged_objs': page['merged_objs'],
                                                    'merge': True
                                                }
                                             }, upsert=False)
            logging.info(f'Updated result: {result}')
        except Exception as e:
            logging.error(f'Document write error: {e}\n Document id: {str(obj["_id"])}')




@click.command()
@click.argument('num_processes')
def click_wrapper(num_processes):
    merge_scan(mongo_insert_fn, int(num_processes))


if __name__ == '__main__':
    click_wrapper()
