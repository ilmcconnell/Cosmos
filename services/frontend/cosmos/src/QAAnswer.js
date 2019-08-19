import React from 'react';
import { makeStyles } from '@material-ui/core/styles';
import Typography from '@material-ui/core/Typography';
import Paper from '@material-ui/core/Paper';

const useStyles = makeStyles(theme => ({
  root: {
    flexGrow: 1,
  },
  container: {
    padding: '2px 4px',
    margin: 20
  },
  demo: {
    backgroundColor: theme.palette.background.paper,
  },
}));



export default function QAAnswer(props){
    const classes = useStyles();
    return (
        <Paper className={classes.container}>
            <Typography variant="p" component="h3">
                {props.answer}
            </Typography>
            <Typography component="p">
              DOI: {props.doi}
            </Typography>
        </Paper>
    )
}
