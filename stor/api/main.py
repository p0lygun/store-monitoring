from typing import Union

from fastapi import FastAPI

app = FastAPI()


@app.get("/trigger_report")
def trigger_report():
    return {"message": "Hello World"}


@app.get("/get_report")
def get_report(report_id: Union[str, int]):
    return {"message": f"Hsello {report_id}"}
