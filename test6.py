from helpers import wJson, rJson
from datetime import datetime

def parse_date(date_str):
    # Try with microseconds first, fallback if not present
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            pass
    raise ValueError(f"Invalid date format: {date_str}")

def sortActivities(activities, field = "completedDate"):
    activities = sorted(
        activities,
        key=lambda x: parse_date(x[field])
    )
    return activities


if __name__ == "__main__":

    data = rJson("jsons/ac4cc77d-2dac-f011-814f-00505690ec8c.json")

    completedActivities = data['completedActivities']

    completedActivities = sortActivities(completedActivities)

    print(completedActivities[0])