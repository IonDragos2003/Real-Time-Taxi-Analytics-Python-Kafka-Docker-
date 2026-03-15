import argparse

from app.utils import setup_topics
from app.producer import run_producer
from app.processor import run_processor
import uvicorn


def parse_args():
    parser = argparse.ArgumentParser(description="Taxi Analytics Pipeline")
    parser.add_argument("--produce",  action="store_true", help="Run the producer")
    parser.add_argument("--process",  action="store_true", help="Run the processor")
    parser.add_argument("--api",      action="store_true", help="Run the API")
    return parser.parse_args()


def main():
    args = parse_args()

    if not any([args.produce, args.process, args.api]):
        print("Nothing to run. Use --produce, --process, or --api (or combine them).")
        return

    setup_topics()

    if args.produce:
        run_producer()

    if args.process:
        run_processor()

    if args.api:
        uvicorn.run("app.api.main:app", host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
