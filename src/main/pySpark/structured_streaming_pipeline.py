from py4j.protocol import Py4JError, Py4JNetworkError

from ApacheSpark.src.main.pySpark.loaders import load_corpus, load_raw_stream, spark
from ApacheSpark.src.main.pySpark.postgres_writer import write_movements_to_postgres, write_cancellations_to_postgres
from ApacheSpark.src.main.pySpark.transforms import extract_cancellations, extract_movements, calculate_delay, \
    add_location_names


def process_batch(raw_df, batch_id):
    print(f"Processing batch {batch_id}")

    corpus_df = load_corpus()

    cancellation_df = extract_cancellations(raw_df)
    movement_df = extract_movements(raw_df)
    delay_df = calculate_delay(movement_df)

    cancellation_with_location = add_location_names(cancellation_df, corpus_df)
    delay_with_location = add_location_names(delay_df, corpus_df)

    write_movements_to_postgres(delay_with_location)
    write_cancellations_to_postgres(cancellation_with_location)

def main():
    raw_stream_df = load_raw_stream()

    query = (
        raw_stream_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", "../../../data/checkpoints/movement_stream")
        .trigger(processingTime="30 seconds")
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Terminating streaming pipeline...")
        query.stop()
    except (Py4JError, Py4JNetworkError, RuntimeError) as e:
        print(f"Streaming query was interrupted during shutdown: {type(e).__name__}")
    finally:
        try:
            if query.isActive:
                query.stop()
        except (Py4JError, Py4JNetworkError, RuntimeError):
            pass
        try:
            spark.stop()
        except (Py4JError, Py4JNetworkError, RuntimeError):
            pass

if __name__ == "__main__":
    main()