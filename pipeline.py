import logging
import apache_beam as beam

def run():
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | 'Create' >> beam.Create([1, 2, 3, 4, 5])
            | 'Print' >> beam.ParDo(lambda x: logging.warning(x))
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.WARNING)
    run()