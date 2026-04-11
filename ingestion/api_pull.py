%pip install requests
import pandas as pd
import numpy as np
import requests

BASE_URL = "https://www.fema.gov/api/open"

ENDPOINTS = {
    "declarations": f"{BASE_URL}/v2/DisasterDeclarationsSummaries",
    "public_assistance": f"{BASE_URL}/v2/PublicAssistanceFundedProjects",
    "disaster_summaries": f"{BASE_URL}/v2/DisasterDeclarationsSummaries",
}