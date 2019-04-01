import click
from torch_model.model.model import MMFasterRCNN
from torch_model.model.utils.config_manager import ConfigManager
import torch
from torch_model.inference.inference import InferenceHelper
from torch_model.inference.data_layer.inference_loader import InferenceLoader
from ingestion.ingest_images import ImageDB



def run_inference(img_dir, proposal_dir, model_config, weights, out_dir, device_str):
    """
    Main function to run inference. Writes a bunch of XMLs to out_dir
    :param img_dir: Input image directory
    :param proposal: Corresponding proposals directory
    :param model_config: Path to model config
    :param weights: path to weights file
    :param out_dir: Path to output directory
    :param device_str: Device config
    """
    cfg = ConfigManager(model_config)
    model = MMFasterRCNN(cfg)
    model.load_state_dict(torch.load(weights, map_location={"cuda:0": device_str}))
    model.eval()
    def bn_train(m):
        if type(m) == torch.nn.BatchNorm2d:
            m.train()
    model.apply(bn_train)
    session, ingest_objs = ImageDB.initialize_and_ingest(img_dir,
                                                         proposal_dir,
                                                         None,
                                                         cfg.WARPED_SIZE,
                                                         'test',
                                                         cfg.EXPANSION_DELTA)
    loader = InferenceLoader(session, ingest_objs, cfg.CLASSES)
    device = torch.device(device_str)
    model.to(device)
    infer_session = InferenceHelper(model, loader, device)
    infer_session.run(out_dir)


@click.command()
@click.argument("img_dir")
@click.argument("proposal_dir")
@click.argument("model_config")
@click.argument("weights")
@click.argument("out_dir")
def run_cli(img_dir, proposal_dir, model_config,weights, out_dir):
    run_inference(img_dir, proposal_dir, model_config,weights, out_dir, 'cuda:0')

if __name__ == "__main__":
    run_cli()
