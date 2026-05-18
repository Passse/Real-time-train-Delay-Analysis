from ApacheSpark.src.main.pySpark.loaders import load_raw_data, load_corpus, spark
from ApacheSpark.src.main.pySpark.postgres_writer import write_cancellations_to_postgres, write_movements_to_postgres
from ApacheSpark.src.main.pySpark.transforms import extract_movements, extract_cancellations, calculate_delay, add_location_names
from ApacheSpark.src.main.pySpark.statistics import show_statistics


def main():
    raw_df = load_raw_data()
    cancellation_df = extract_cancellations(raw_df)
    movement_df = extract_movements(raw_df)
    delay_df = calculate_delay(movement_df)
    corpus_df = load_corpus()

    movement_with_location = add_location_names(movement_df, corpus_df)
    cancellation_with_location = add_location_names(cancellation_df, corpus_df)
    delay_with_location = add_location_names(delay_df, corpus_df)

    show_statistics(cancellation_with_location, movement_with_location, delay_with_location)
    # cancellation_with_location.show(truncate=False)
    # delay_with_location.show(truncate=False)

    write_movements_to_postgres(delay_with_location)
    write_cancellations_to_postgres(cancellation_with_location)

if __name__ == "__main__":
    main()
    spark.stop()