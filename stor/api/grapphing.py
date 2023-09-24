from typing import Dict
import csv
from bokeh.embed import components
from bokeh.plotting import figure, curdoc
from bokeh.resources import INLINE
from bokeh.models import ColumnDataSource
from random import choices

from fastapi import Request, BackgroundTasks
import numpy as np

from .. import config

from ..report import generate_total_report


def get_graph(background_tasks: BackgroundTasks):

    total_report_file = config.REPORT_CACHE_DIR / 'total_report.csv'
    if not total_report_file.exists():
        if not config.GENERATING_REPORTS:
            background_tasks.add_task(generate_total_report)
            config.GENERATING_REPORTS = True

    if config.GENERATING_REPORTS:
        p = figure(
            title="Generating report...",
            x_axis_label="x",
            y_axis_label="y"
        )
        return p

    stores = []
    uptime_min, uptime_max = 0, 0
    downtime_min, downtime_max = 0, 0
    CLAMP_MAX = 50
    RANDOM_SECTION_SIZE = 20

    with open(total_report_file, 'r') as f:
        reader = csv.reader(f)
        for index, row in enumerate(reader):
            if index == 0:
                continue
            store_id = str(row[0])
            uptime, downtime = float(row[1]), float(row[2])
            uptime_max = max(uptime_max, uptime)
            uptime_min = min(uptime_min, uptime)
            downtime_max = max(downtime_max, downtime)
            downtime_min = min(downtime_min, downtime)
            stores.append((store_id, uptime, downtime))

    stores = choices(stores, k=RANDOM_SECTION_SIZE)

    store_uptime = [store[1] for store in stores]
    store_downtime = [store[2] for store in stores]
    stores = [store[0] for store in stores]

    store_uptime = np.interp(store_uptime, (uptime_min, uptime_max), (0, CLAMP_MAX))
    store_downtime = np.interp(store_downtime, (downtime_min, downtime_max), (0, -CLAMP_MAX))

    uptime = {
        'stores': stores,
        'uptime': store_uptime,
    }
    downtime = {
        'stores': stores,
        'downtime': store_downtime,
    }
    p = figure(
        y_range=stores,
        title=f"Store Status for {RANDOM_SECTION_SIZE} random stores",
        toolbar_location="below",
        sizing_mode="stretch_width",
        tooltips=[("Store", "@stores"), ("Uptime", "@uptime")],
        x_range=(-CLAMP_MAX*1.5, CLAMP_MAX*1.5)
    )

    p.hbar_stack(
        ["uptime"],
        y='stores',
        height=0.1,
        color=["#18ba20"],
        source=ColumnDataSource(data=uptime),
    )
    p.hbar_stack(
        ["downtime"],
        y='stores',
        height=0.1,
        color=["#c93810"],
        source=ColumnDataSource(data=downtime),
    )

    p.y_range.range_padding = 0.1
    p.ygrid.grid_line_color = None
    p.axis.minor_tick_line_color = None
    p.outline_line_color = None
    p.legend.location = "top_left"
    p.legend.orientation = "horizontal"

    return p


def get_graph_template_options_dict(request: Request, background_tasks) -> Dict:
    js_resources = INLINE.render_js()
    css_resources = INLINE.render_css()

    graph = get_graph(background_tasks)
    curdoc().theme = 'dark_minimal'
    curdoc().add_root(graph)

    script, div = components(graph, INLINE)

    return {
            "request": request,
            "plot_script": script,
            "plot_div": div,
            "js_resources": js_resources,
            "css_resources": css_resources
    }
