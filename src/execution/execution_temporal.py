import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))


from src.pipeline.pipe_temporal import *



def main():
    """Disaggregate the yearly consumption to a temporal resolution"""

    year = 2045
    energy_carrier = "petrol"
    force_preprocessing = True
    sector = "industry"

    df = disaggregate_temporal(year=year, energy_carrier=energy_carrier, force_preprocessing=force_preprocessing, sector=sector)

    print(df)


if __name__ == "__main__":
    main()


