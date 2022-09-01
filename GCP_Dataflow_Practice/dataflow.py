import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputgcs",
                        dest="inputgcs",
                        required=True,
                        help="Please provide input gcs path")
    parser.add_argument("--outputpath",
                        dest="outputpath",
                        required=True,
                        help="Please provide output gcs path")

    known_args , pipline_args = parser.parse_known_args()

    pipeline_option = PipelineOptions(pipline_args)
    pipeline_option.view_as(SetupOptions).save_main_session = True

    def test(element):
        print("=====================")
        print(element)
        print("=====================")


    with beam.Pipeline(options=pipeline_option) as p:
        line = p | "Read" >> ReadFromText(known_args.inputgcs)
        sumval = (line | "split" >> beam.FlatMap(lambda  x: x.spilt(","))
                        | "pairwith" >> beam.Map(lambda x:(x,1)))

