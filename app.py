import os

# Disable analytics, telemetry, and offline mode
os.environ["GRADIO_ANALYTICS_ENABLED"] = "False"
os.environ["HF_HUB_OFFLINE"] = "1"
os.environ["TRANSFORMERS_OFFLINE"] = "1"
os.environ["DISABLE_TELEMETRY"] = "1"
os.environ["DO_NOT_TRACK"] = "1"
os.environ["HF_HUB_DISABLE_IMPLICIT_TOKEN"] = "1"
os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"

import gradio as gr
import polars as pl
from datetime import datetime
from backend.optimization import get_optimal_stop, get_actual_time_optimal_stop
from backend.utils import (
    get_next_meetup_time,
    validate_date_time,
    get_total_minutes_with_retries,
)


def validate_inputs(num_stops, top_geo, top_time, display_top_val, selected_stops, all_stops_list):
    """Validate input values to prevent absurdly high values that could cause performance issues."""
    errors = []
    
    # Validate number of stops
    if not isinstance(num_stops, (int, float)) or num_stops < 2:
        errors.append("Number of stops must be at least 2.")
    elif num_stops > 20:
        errors.append(f"Number of stops ({num_stops}) exceeds maximum allowed (20).")
    
    # Validate top_geo
    if not isinstance(top_geo, (int, float)) or top_geo < 1:
        errors.append("Geo Results must be at least 1.")
    elif top_geo > 100:
        errors.append(f"Geo Results ({top_geo}) exceeds maximum allowed (100).")
    
    # Validate top_time
    if not isinstance(top_time, (int, float)) or top_time < 1:
        errors.append("Time Results must be at least 1.")
    elif top_time > 100:
        errors.append(f"Time Results ({top_time}) exceeds maximum allowed (100).")
    
    # Validate display_top
    if not isinstance(display_top_val, (int, float)) or display_top_val < 1:
        errors.append("Display Top must be at least 1.")
    elif display_top_val > 100:
        errors.append(f"Display Top ({display_top_val}) exceeds maximum allowed (100).")
    
    # Validate selected stops
    if not selected_stops or len(selected_stops) == 0:
        errors.append("At least one stop must be selected.")
    elif len(selected_stops) != int(num_stops):
        errors.append(f"Number of selected stops ({len(selected_stops)}) does not match number of stops ({int(num_stops)}).")
    
    # Check if all selected stops are valid (non-empty and in all_stops_list)
    if all_stops_list:
        invalid_stops = [stop for stop in selected_stops if not stop or stop not in all_stops_list]
        if invalid_stops:
            errors.append(f"Invalid stops selected: {invalid_stops[:3]}{'...' if len(invalid_stops) > 3 else ''}")
    
    # Estimate total API calls to prevent excessive usage
    # Rough estimate: num_stops * (top_geo + top_time) for candidate selection
    # Then num_stops * len(union_of_candidates) for actual time queries
    # Worst case: num_stops * (top_geo + top_time) + num_stops * (top_geo + top_time)
    estimated_api_calls = int(num_stops) * (int(top_geo) + int(top_time)) * 2
    max_api_calls = 5000  # Reasonable limit to prevent abuse
    
    if estimated_api_calls > max_api_calls:
        errors.append(
            f"Request would result in too many API calls (estimated: {estimated_api_calls}). "
            f"Please reduce the number of stops or result limits."
        )
    
    return len(errors) == 0, errors


def cerate_app():
    with gr.Blocks() as app:
        gr.Markdown("## Optimal Public Transport Stop Finder in Prague")
        gr.Markdown(
            """
        Consider you are in Prague and you want to meet with your friends. What is the optimal stop to meet? Now you can find that with this app!
        
        Time table data are being scraped from IDOS API, IDOS uses PID timetable data."""
        )
        
        with gr.Accordion("How Does It Work?", open=False):
            gr.Markdown(
                """
### Problem Formulation

Given a set of **k** starting stops **S = {s₁, s₂, ..., sₖ}**, we want to find an optimal target stop **t*** ∈ **T** (where **T** is the set of all 1463 stops in Prague) that minimizes travel distance/time for all participants.

### Distance Functions

We define two distance metrics:

1. **Geographic Distance**: `d_geo(sᵢ, t) = ||GPS(sᵢ) - GPS(t)||` (distance between stops on a globein kilometers)

2. **Temporal Distance**: `d_time(sᵢ, t, dt) = travel_time(sᵢ → t, dt)` (minutes to travel from sᵢ to t at datetime dt)

The temporal distance is computed by scraping actual public transport schedules from IDOS/DPP APIs for ~2.1M stop combinations.

### Optimization Objectives

We consider two optimization criteria:

#### 1. Minimize Worst Case (Min-Max)
Find **t*** that minimizes the maximum travel time/distance:

```
t* = argmin_{t ∈ T} max_{i=1..k} d(sᵢ, t)
```

Where `d(sᵢ, t)` can be either `d_geo(sᵢ, t)` or `d_time(sᵢ, t, dt)`.

**Objective function**: `f_worst(t) = max(d(s₁, t), d(s₂, t), ..., d(sₖ, t))`

#### 2. Minimize Total Time (Sum)
Find **t*** that minimizes the sum of all travel times/distances:

```
t* = argmin_{t ∈ T} Σ_{i=1}^{k} d(sᵢ, t)
```

**Objective function**: `f_total(t) = Σ_{i=1}^{k} d(sᵢ, t)`

### Algorithm

The algorithm uses a two-stage optimization approach:

**Stage 1: Candidate Selection**
- Select top **N_geo** candidates based on geographic distance: `C_geo = top_N_geo(argmin_{t ∈ T} f(d_geo(sᵢ, t)))`
- Select top **N_time** candidates based on pre-scraped temporal distance: `C_time = top_N_time(argmin_{t ∈ T} f(d_time(sᵢ, t, dt_ref)))`
- Union: `C = C_geo ∪ C_time`

**Stage 2: Real-Time Refinement**
- For each candidate **c ∈ C**, query actual travel times for the specified datetime **dt**:
  - `d_actual(sᵢ, c, dt) = get_total_minutes(sᵢ, c, dt)` ∀ **i ∈ {1..k}**
- Compute objective values:
  - `f_worst(c) = max(d_actual(s₁, c, dt), ..., d_actual(sₖ, c, dt))`
  - `f_total(c) = Σ_{i=1}^{k} d_actual(sᵢ, c, dt)`
- Sort candidates by selected objective and return top **N_display** results

### Complexity

- **Space**: O(|T|²) for pre-computed distance matrix
- **Time**: O(k × |C| × API_latency) for real-time queries
- **API Calls**: k × |C| queries to IDOS/DPP APIs

### Default Parameters

- **N_geo** = 10 (top geographic candidates)
- **N_time** = 25 (top temporal candidates)  
- **N_display** = 15 (final results shown)
                """
            )

        with gr.Row():
            number_of_stops = gr.Number(
                minimum=2, maximum=12, step=1, value=3, label="Number of People"
            )

            method = gr.Dropdown(
                choices=["Minimize worst case for each", "Minimize total time"],
                value="Minimize worst case for each",
                label="Optimization Method",
            )

            show_top_geo = gr.Number(
                minimum=5, maximum=50, step=1, value=10, label="Geo Results"
            )

            show_top_time = gr.Number(
                minimum=5, maximum=50, step=1, value=25, label="Time Results"
            )

            display_top = gr.Number(
                minimum=5, maximum=50, step=1, value=15, label="Display Top"
            )

        next_dt = get_next_meetup_time(4, 20)  # Friday 20:00
        next_date = next_dt.strftime("%d/%m/%Y")
        next_time = next_dt.strftime("%H:%M")
        with gr.Row():
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
            num_stops, chosen_method, date_str, time_str, top_geo, top_time, display_top_val, *all_stops
        ):
            # Validate date and time first
            is_valid, error_message = validate_date_time(date_str, time_str)
            if not is_valid:
                raise gr.Error(error_message)

            # Extract selected stops before validation
            selected_stops = [stop for stop in all_stops[:int(num_stops)] if stop]
            
            # Validate all input values before processing
            is_valid, validation_errors = validate_inputs(
                num_stops, top_geo, top_time, display_top_val, selected_stops, ALL_STOPS
            )
            if not is_valid:
                error_message = "Input validation failed:\n" + "\n".join(f"- {err}" for err in validation_errors)
                raise gr.Error(error_message)
            
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
                DISTANCE_TABLE, method, selected_stops, show_top_geo=int(top_geo), show_top_time=int(top_time)
            )
            print(target_stops)
            df_times = get_actual_time_optimal_stop(
                method, 
                selected_stops, 
                target_stops, 
                event_datetime, 
                get_total_minutes_with_retries,
                show_top=int(display_top_val)
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
            inputs=[number_of_stops, method, date_input, time_input, show_top_geo, show_top_time, display_top] + dropdowns,
            outputs=results_table,
            api_name=False
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
        **Feedback**: Help me improve the app by sharing your experience [here](https://docs.google.com/forms/d/e/1FAIpQLSeXq6DXWkjcsgs4XRPN0VnccThMwjDQP2Si25MMB76yW14tZA/viewform?usp=dialog).
        """
        )
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
