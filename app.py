import gradio as gr
import polars as pl
from datetime import datetime
from backend.optimization import get_optimal_stop, get_actual_time_optimal_stop
from backend.utils import (
    get_next_meetup_time,
    validate_date_time,
    get_total_minutes_with_retries,
)


def cerate_app():
    with gr.Blocks() as app:
        gr.Markdown("## Optimal Public Transport Stop Finder in Prague")
        gr.Markdown(
            """
        Consider you are in Prague and you want to meet with your friends. What is the optimal stop to meet? Now you can find that with this app!
        
        Time table data are being scraped from IDOS API, IDOS uses PID timetable data."""
        )

        number_of_stops = gr.Slider(
            minimum=2, maximum=12, step=1, value=3, label="Number of People"
        )

        method = gr.Radio(
            choices=["Minimize worst case for each", "Minimize total time"],
            value="Minimize worst case for each",
            label="Optimization Method",
        )

        next_dt = get_next_meetup_time(4, 20)  # Friday 20:00
        next_date = next_dt.strftime("%d/%m/%Y")
        next_time = next_dt.strftime("%H:%M")
        date_input = gr.Textbox(
            label="Date (DD/MM/YYYY)", placeholder=f"e.g., {next_date}", value=next_date
        )

        time_input = gr.Textbox(
            label="Time (HH:MM)", placeholder=f"e.g., {next_time}", value=next_time
        )

        dropdowns = []
        for i in range(12):
            dd = gr.Dropdown(
                choices=ALL_STOPS, label=f"Choose Starting Stop #{i+1}", visible=False
            )
            dropdowns.append(dd)

        def update_dropdowns(n):
            updates = []
            for i in range(12):
                if i < n:
                    updates.append(gr.update(visible=True))
                else:
                    updates.append(gr.update(visible=False))
            return updates

        number_of_stops.change(
            fn=update_dropdowns, inputs=number_of_stops, outputs=dropdowns
        )

        search_button = gr.Button("Search")

        def search_optimal_stop(
            num_stops, chosen_method, date_str, time_str, *all_stops
        ):
            is_valid, error_message = validate_date_time(date_str, time_str)
            if not is_valid:
                raise gr.Error(error_message)

            selected_stops = [stop for stop in all_stops[:num_stops] if stop]
            print("Number of stops:", num_stops)
            print("Method selected:", chosen_method)
            print("Selected stops:", selected_stops)
            print("Selected date:", date_str)
            print("Selected time:", time_str)

            if chosen_method == "Minimize worst case for each":
                method = "minimize-worst-case"
            else:
                method = "minimize-total"

            try:
                event_datetime = datetime.strptime(
                    f"{date_str} {time_str}", "%d/%m/%Y %H:%M"
                )
                print("Event DateTime:", event_datetime)
            except ValueError as e:
                raise gr.Error(f"Error parsing date and time: {e}")

            target_stops = get_optimal_stop(
                DISTANCE_TABLE, method, selected_stops, show_top_geo=10, show_top_time=SHOW_TOP+10
            )
            print(target_stops)
            df_times = get_actual_time_optimal_stop(
                method, 
                selected_stops, 
                target_stops, 
                event_datetime, 
                get_total_minutes_with_retries,
                show_top=SHOW_TOP
            )
            df_times = df_times.with_row_index("#", offset=1)

            return df_times

        results_table = gr.Dataframe(
            headers=["Target Stop", "Worst Case Minutes", "Total Minutes"],
            datatype=["str", "number", "str"],
            label="Optimal Stops",
        )

        search_button.click(
            fn=search_optimal_stop,
            inputs=[number_of_stops, method, date_input, time_input] + dropdowns,
            outputs=results_table,
        )

        app.load(
            lambda: [gr.update(visible=True) for _ in range(3)]
            + [gr.update(visible=False) for _ in range(9)],
            inputs=[],
            outputs=dropdowns,
        )

        gr.Markdown("---")
        gr.Markdown(
            """
        Created by [Daniel Herman](https://www.hermandaniel.com), check out the code [detrin/pub-finder](https://github.com/detrin/pub-finder).
        """
        )
    return app


stops_geo_dist = pl.read_parquet("Prague_stops_combinations.parquet")
print(stops_geo_dist)
DISTANCE_TABLE = stops_geo_dist
from_stops = DISTANCE_TABLE["from"].unique().sort().to_list()
to_stops = DISTANCE_TABLE["to"].unique().sort().to_list()
ALL_STOPS = sorted(list(set(from_stops) & set(to_stops)))
SHOW_TOP = 15

if __name__ == "__main__":
    app = cerate_app()
    print("Starting app ...")
    app.launch(server_name="0.0.0.0", server_port=3000)
