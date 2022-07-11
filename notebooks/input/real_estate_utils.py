import requests
from bs4 import BeautifulSoup
import re
from typing import List
import json
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def get_last_page_number(city: str, radius: int) -> int:
    """Gets last number of the various pages rendered on the front end"""

    url = f"https://www.immoscout24.ch/en/house/buy/city-{city}?r={radius}&map=1"

    html = requests.get(url)
    soup = BeautifulSoup(html.text, "html.parser")
    buttons = soup.findAll("button")

    p = []
    for item in buttons:
        if len(item.text) <= 3 & len(item.text) != 0:
            p.append(item.text)
    if p:
        last_page = int(p.pop())
    else:
        last_page = 1

    return last_page


def get_properties_ids(city: str, radius: int) -> List[str]:
    """Gets the properties listed IDs"""
    last_page = get_last_page_number(city, radius)

    ids = []

    for pn in range(1, last_page + 1):
        url = f"https://www.immoscout24.ch/en/house/buy/city-{city}?pn={pn}&r={radius}&map=1"

        html = requests.get(url)
        soup = BeautifulSoup(html.text, "html.parser")
        links = soup.findAll("a", href=True)
        hrefs = [item["href"] for item in links]
        hrefs_filtered = [href for href in hrefs if href.startswith("/en/d")]
        ids += [re.findall("\d+", item)[0] for item in hrefs_filtered]

    return ids


def get_property_metadata(property_id: str) -> dict:
    """Gets a particular Property ID metadata"""

    url = f"https://rest-api.immoscout24.ch/v4/en/properties/{property_id}"
    response = requests.get(url)
    property_metadata = json.loads(response.text)

    return property_metadata


def get_house_characteristic(property_metadata: dict, dict_path: List[str]) -> str:
    """Gets the value, if exists, following the desired dict path"""
    value = property_metadata
    try:
        for route in dict_path:
            value = value.get(route)

    except:
        value = None

    return value


def parse_property_metadata(property_id: int) -> pd.DataFrame:
    """Parses the Property API response into a Pandas DataFrame"""

    try:
        property_metadata = get_property_metadata(property_id)

        attributes_size_volume = get_house_characteristic(
            property_metadata, ["propertyDetails", "attributesSize", "volume"]
        )
        attributes_size_number_of_floors = get_house_characteristic(
            property_metadata, ["propertyDetails", "attributesSize", "numberOfFloors"]
        )
        attributes_inside_animal_allowed = get_house_characteristic(
            property_metadata, ["propertyDetails", "attributesInside", "animalAllowed"]
        )
        attributes_inside_bathrooms = get_house_characteristic(
            property_metadata, ["propertyDetails", "attributesInside", "nrBathRooms"]
        )
        attributes_inside_attic = get_house_characteristic(
            property_metadata, ["propertyDetails", "attributesInside", "attic"]
        )
        attributes_inside_cellar = get_house_characteristic(
            property_metadata, ["propertyDetails", "attributesInside", "cellar"]
        )
        attributes_technology_dishwasher = get_house_characteristic(
            property_metadata, ["propertyDetails", "attributesTechnology", "dishWasher"]
        )
        attributes_technology_cable_tv = get_house_characteristic(
            property_metadata,
            ["propertyDetails", "attributesTechnology", "propCabletv"],
        )
        attributes_outside_balcony = get_house_characteristic(
            property_metadata, ["propertyDetails", "attributesOutside", "propBalcony"]
        )
        attributes_outside_playground = get_house_characteristic(
            property_metadata, ["propertyDetails", "attributesOutside", "playGround"]
        )
        attributes_outside_parking = get_house_characteristic(
            property_metadata, ["propertyDetails", "attributesOutside", "propParking"]
        )
        attributes_outside_garage = get_house_characteristic(
            property_metadata, ["propertyDetails", "attributesOutside", "propGarage"]
        )
        attributes_surrounding_distance_shop = get_house_characteristic(
            property_metadata,
            ["propertyDetails", "attributesSurrounding", "distanceShop"],
        )
        attributes_surrounding_distance_kindergarten = get_house_characteristic(
            property_metadata,
            ["propertyDetails", "attributesSurrounding", "distanceKindergarten"],
        )
        attributes_surrounding_distance_school_1 = get_house_characteristic(
            property_metadata,
            ["propertyDetails", "attributesSurrounding", "distanceSchool1"],
        )
        attributes_surrounding_distance_school_2 = get_house_characteristic(
            property_metadata,
            ["propertyDetails", "attributesSurrounding", "distanceSchool2"],
        )
        attributes_surrounding_distance_public_transport = get_house_characteristic(
            property_metadata,
            ["propertyDetails", "attributesSurrounding", "distancePublicTransport"],
        )
        attributes_surrounding_distance_motorway = get_house_characteristic(
            property_metadata,
            ["propertyDetails", "attributesSurrounding", "distanceMotorway"],
        )
        attributes_new_building = get_house_characteristic(
            property_metadata,
            ["propertyDetails", "attributesSurrounding", "newBuilding"],
        )
        attributes_new_year_built = get_house_characteristic(
            property_metadata, ["propertyDetails", "attributesSurrounding", "yearBuilt"]
        )
        normalized_price = get_house_characteristic(
            property_metadata, ["propertyDetails", "normalizedPrice"]
        )
        number_of_rooms = get_house_characteristic(
            property_metadata, ["propertyDetails", "numberOfRooms"]
        )
        state = get_house_characteristic(
            property_metadata, ["propertyDetails", "state"]
        )
        surface_property = get_house_characteristic(
            property_metadata, ["propertyDetails", "surfaceProperty"]
        )
        surface_usable = get_house_characteristic(
            property_metadata, ["propertyDetails", "surfaceUsable"]
        )
        surface_living = get_house_characteristic(
            property_metadata, ["propertyDetails", "surfaceLiving"]
        )

        property_data = pd.DataFrame(
            {
                "property_id": [property_id],
                "attributes_size_volume": [attributes_size_volume],
                "attributes_size_number_of_floors": [attributes_size_number_of_floors],
                "attributes_inside_animal_allowed": [attributes_inside_animal_allowed],
                "attributes_inside_bathrooms": [attributes_inside_bathrooms],
                "attributes_inside_attic": [attributes_inside_attic],
                "attributes_inside_cellar": [attributes_inside_cellar],
                "attributes_technology_dishwasher": [attributes_technology_dishwasher],
                "attributes_technology_cable_tv": [attributes_technology_cable_tv],
                "attributes_outside_balcony": [attributes_outside_balcony],
                "attributes_outside_playground": [attributes_outside_playground],
                "attributes_outside_parking": [attributes_outside_parking],
                "attributes_outside_garage": [attributes_outside_garage],
                "attributes_surrounding_distance_shop": [
                    attributes_surrounding_distance_shop
                ],
                "attributes_surrounding_distance_kindergarten": [
                    attributes_surrounding_distance_kindergarten
                ],
                "attributes_surrounding_distance_school_1": [
                    attributes_surrounding_distance_school_1
                ],
                "attributes_surrounding_distance_school_2": [
                    attributes_surrounding_distance_school_2
                ],
                "attributes_surrounding_distance_public_transport": [
                    attributes_surrounding_distance_public_transport
                ],
                "attributes_surrounding_distance_motorway": [
                    attributes_surrounding_distance_motorway
                ],
                "attributes_new_building": [attributes_new_building],
                "attributes_new_year_built": [attributes_new_year_built],
                "normalized_price": [normalized_price],
                "number_of_rooms": [number_of_rooms],
                "surface_property": [surface_property],
                "surface_usable": [surface_usable],
                "surface_living": [surface_living],
                "request_response": [property_metadata],
            }
        )

    except Exception as e:

        logger.error(f"Error processing Property ID {property_id}")
        logger.error(e)

        property_data = pd.DataFrame(
            {
                "property_id": [property_id],
                "attributes_size_volume": [None],
                "attributes_size_number_of_floors": [None],
                "attributes_inside_animal_allowed": [None],
                "attributes_inside_bathrooms": [None],
                "attributes_inside_attic": [None],
                "attributes_inside_cellar": [None],
                "attributes_technology_dishwasher": [None],
                "attributes_technology_cable_tv": [None],
                "attributes_outside_balcony": [None],
                "attributes_outside_playground": [None],
                "attributes_outside_parking": [None],
                "attributes_outside_garage": [None],
                "attributes_surrounding_distance_shop": [None],
                "attributes_surrounding_distance_kindergarten": [None],
                "attributes_surrounding_distance_school_1": [None],
                "attributes_surrounding_distance_school_2": [None],
                "attributes_surrounding_distance_public_transport": [None],
                "attributes_surrounding_distance_motorway": [None],
                "attributes_new_building": [None],
                "attributes_new_year_built": [None],
                "normalized_price": [None],
                "number_of_rooms": [None],
                "surface_property": [None],
                "surface_usable": [None],
                "surface_living": [None],
                "request_response": [None],
            }
        )

    logger.info(f"{property_id} ready.")
    return property_data
