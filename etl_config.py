import json
from logging.config import dictConfig

staging_table_data_config = {
    'staging_songs_table': {
        'path': 's3://udacity-dend/song_data',
        'json_path_file': 'auto',
        'region': 'us-west-2'
    },
    'staging_events_table': {
        'path': 's3://udacity-dend/log_data',
        'json_path_file': 's3://udacity-dend/log_json_path.json',
        'region': 'us-west-2'
    }
}

iam_config = {
    'role_path': '/',
    'role_name': 'redshift_s3_access',
    'role_description': 'Allows Redshift clusters to call AWS services on your behalf.',
    'policy_document': json.dumps(
        {
            'Statement':
                [
                    {
                        'Effect': 'Allow',
                        'Action': 'sts:AssumeRole',
                        'Principal': {'Service': 'redshift.amazonaws.com'}
                    }
                ],
            'Version': '2012-10-17'
        }
    ),
    'policyARNs': [
        "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    ]
}

redshift_config = {
    'cluster_type': 'multi-node',
    'node_type': 'dc2.large',
    'num_nodes': 2,
    'db_name': 'sparkify_schema',
    'db_user': 'music_analysts',
    'db_password': '2pacftEminem',
    'db_port': 5439,
    'iam_role_name': 'redshift_s3_access',
    'cluster_identifier': 'sparkifycluster',
    'ipv4_cidr_range': '0.0.0.0/0',
    'ip_protocol': 'TCP'
}

logging_config = {
    'version': 1,
    'formatters': {
        'standard': {
            'format': '%(asctime)-15s [%(levelname)s] %(name)s|%(lineno)s:: %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
        },
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': False
        },
        'db_manager': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': False
        },
        '__main__': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': False
        },
    }
}

dictConfig(logging_config)
