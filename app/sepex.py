import os
from datetime import datetime
from typing import Any, List, Optional

import pandas as pd
import requests
from pydantic import BaseModel


class JobResultsResponse(BaseModel):
    jobID: str
    processID: str
    results: dict


BASE_URL = os.getenv("SEPEX_BASE_URL", "http://localhost:5050")


class SepexAPI:
    def __init__(self, base_url: str = BASE_URL):
        self.base_url = base_url.rstrip("/")

    def fetch_table(self, endpoint: str, model_cls: BaseModel, params: dict = None) -> pd.DataFrame:
        url = f"{self.base_url}/{endpoint}"
        # print(f"Fetching data from {url} with params {params}")
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict):
                for v in data.values():
                    if isinstance(v, list):
                        data = v
                        break
            items = [model_cls(**item) for item in data]
            return pd.DataFrame([item.model_dump() for item in items])
        else:
            print(f"Failed to fetch data from {endpoint}: {response.status_code}")
            return None

    def fetch_processes_yaml(self, endpoint: str = "processes") -> str:
        import yaml

        url = f"{self.base_url}/{endpoint}"
        # print(f"Fetching processes from {url}")
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            # print("data:", data)
            # Expecting dict with 'processes' key
            processes = data.get("processes", [])
            process_ids = [p["id"] for p in processes]
            # print(f"Fetched processes: {process_ids}")
            yamls = yaml.dump([Process(**p).model_dump() for p in processes], sort_keys=False)
            return process_ids, yamls
        else:
            print(f"Failed to fetch processes: {response.status_code}")
            return None

    def fetch_processes_dict(self, endpoint: str = "processes") -> dict:
        url = f"{self.base_url}/{endpoint}"
        # print(f"Fetching processes from {url}")
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            processes = data.get("processes", [])
            # Return dict keyed by process id
            return {p["id"]: Process(**p).model_dump() for p in processes if "id" in p}
        else:
            print(f"Failed to fetch processes: {response.status_code}")
            return {}


class Process(BaseModel):
    version: str
    id: str
    title: str
    description: str
    jobControlOptions: List[str]
    outputTransmission: Optional[Any]


class ProcessesResponse(BaseModel):
    links: List[Any]
    processes: List[Process]


class Job(BaseModel):
    jobID: str
    updated: datetime
    status: str
    processID: str
    type: Optional[str]
    submitter: Optional[str]


class JobsResponse(BaseModel):
    jobs: List[Job]
    links: List[Any]


class ProcessInfo(BaseModel):
    version: str
    id: str
    title: str
    description: str
    jobControlOptions: List[str]
    outputTransmission: Optional[Any]


class CommandInput(BaseModel):
    id: str
    title: str
    description: str
    input: dict
    minOccurs: int
    maxOccurs: int


class CommandOutput(BaseModel):
    id: str
    title: str
    description: str
    output: dict


class ProcessDetailResponse(BaseModel):
    info: ProcessInfo
    command: List[str]
    inputs: List[CommandInput]
    outputs: List[CommandOutput]
    links: Optional[Any]


class JobDetail(BaseModel):
    jobID: str
    updated: datetime
    status: str
    processID: str


class LogEntry(BaseModel):
    level: str
    msg: str
    time: str


class JobLogsResponse(BaseModel):
    jobID: str
    processID: str
    status: str
    process_logs: List[LogEntry]
    server_logs: List[LogEntry]


class ImageInfo(BaseModel):
    imageDigest: str
    imageURI: str


class ProcessMeta(BaseModel):
    processId: str
    processVersion: str


class JobMetadataResponse(BaseModel):
    context: str
    apiJobId: str
    commands: List[str]
    endedAtTime: str
    generatedAtTime: str
    image: ImageInfo
    process: ProcessMeta
    startedAtTime: str
