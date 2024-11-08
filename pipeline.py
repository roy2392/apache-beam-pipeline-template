import logging
import apache_beam as beam
import argparse

def run(argv=None):
        parser = argparse.ArgumentParser()
        parser.add_argument('--input')
        parser.add_argument('--output')
        args, beam_args = parser.parse_known_args(argv)
        with beam.Pipeline(argv = beam_args) as p:
            lines = p | "ReadFile" >> beam.io.ReadFromText(args.input, skip_header_lines=1)
            lines | beam.Map(print)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.WARNING)
    run()