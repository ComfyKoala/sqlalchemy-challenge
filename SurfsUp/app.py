# Import the dependencies.
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

import pandas as pd
import datetime as dt
import numpy as np

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine)

# Save references to each table
measurement = Base.classes.measurement
station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Helper Functions
#################################################

# returns the date a year before the most recent date in the database
def lastYear():
    # Starting from the most recent data point in the database. 
    most_recent_dt = session.query(measurement).order_by(measurement.date.desc()).first().date
    # Calculate the date one year from the last date in data set.
    year_ago_dt = dt.datetime.fromisoformat(most_recent_dt) - dt.timedelta(days=365)
    # need to remove 00:00:00 timestamp from saved year_ago_dt because SQLite does not have Date types, only String comparisons
    return(year_ago_dt.strftime('%Y-%m-%d'))

# returns most active station code
def mostActiveStation():
    # List the stations and their counts in descending order.
    active_stations = session.query(measurement.station, func.count(measurement.station))\
        .group_by(measurement.station)\
        .order_by(func.count(measurement.station).desc()).all()
    # because stations are ordered from highest to least count, most_active is the first entry
    return(active_stations[0][0])

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route("/")
def home():
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/:start:<br/>"
        f"/api/v1.0/:start:/:end:"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Perform a query to retrieve the date and precipitation scores
    select = [measurement.date, measurement.prcp]
    results = session.query(*select).where(measurement.date >= lastYear())

    # create empty dictionary to store key-val sets
    past_yr_precip_dict = {}
    for row in results:
        # only add entry if not empty
        if row.prcp is not None:
            # if row doesn't exist, add first entry as list
            if row.date not in past_yr_precip_dict:
                past_yr_precip_dict[row.date] = [row.prcp]
            # if row already exists, append to the existing list
            else:
                past_yr_precip_dict[row.date].append(row.prcp)

    # return jsonified dictionary
    return jsonify(past_yr_precip_dict)

@app.route("/api/v1.0/stations")
def stations():
    # get all stations
    results = session.query(station.station).all()
    return jsonify({'stations':list(np.ravel(results))})

@app.route("/api/v1.0/tobs")
def tobs():
    # get temperature data for the most active station for the past year
    results = session.query(measurement.tobs).where(measurement.station == mostActiveStation())\
        .where(measurement.date >= lastYear()).all()
    return jsonify({'temp_observations':list(np.ravel(results))})

@app.route("/api/v1.0/<start>")
@app.route("/api/v1.0/<start>/<end>")
def temp_range(start=None, end=None):
    # format the provided (or defaulted) values. if ValueError is raised, not formatted properly
    try:
        dt.datetime.fromisoformat(start)
        if end is not None:
            dt.datetime.fromisoformat(end)
    # fail cases: improperly formatted date, date not found (e.g. february 53rd)
    except ValueError:
        return jsonify({"error": f"Date value must be in 'YYYY-MM-DD' format or date entered is invalid."}), 400

    # check if start is before end
    if start > end:
        return jsonify({"error": f"Start date value must be before end date."}), 400

    select = [measurement.date,
        func.min(measurement.tobs),
        func.avg(measurement.tobs),
        func.max(measurement.tobs)]
    # if end is empty, only start is defined for filtering
    if end is None:
        results = session.query(*select).where(measurement.date >= start).group_by(measurement.date).all()
    else:
        results = session.query(*select).where(measurement.date >= start)\
            .where(measurement.date <= end).group_by(measurement.date).all()
    
    # store results in DataFrame for to_dict function
    results_df = pd.DataFrame(results, columns=['date', 'tmin', 'tavg', 'tmax'])
    results_df = results_df.set_index('date')

    return jsonify(results_df.to_dict('index'))

#################################################
# Run Flask
#################################################
if __name__ == '__main__':
    app.run(debug=True)

session.close()