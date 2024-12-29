#!/usr/bin/env python3
# This script was created for a technical exercise.
# The data used is from TheCocktailDB API. This is a free to use dataset
# This script will process the json data from the api normalise and standardise the output
# and outputs both raw and transformed data to files.
# These files can be used for further analysis and reporting using the transformed data 
# or verification using the original raw data.

import requests
import json
import re

# 1 Data Ingestion
def fetch_cocktail_data(): # Fetch data, exception status_code =! 200
    url = "https://www.thecocktaildb.com/api/json/v1/1/random.php"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()["drinks"][0]  # Extract the cocktail data
    else:
        raise Exception(f"Failed to fetch data. Status code: {response.status_code}")
    
def standardise_units(measures): # Standardise units into SI (ml)
    unit_conversion = {
        "oz": 29.57,
        "ml": 1.0,
        "cl": 10.0,
        "tsp": 4.93,
        "tbsp": 14.79,
    }
    standardised_measures = []

    for measure in measures:
        if measure:
            match = re.match(r"(\d+(\.\d+)?)\s*(\w+)", measure) # Match: one or more digit followed by optional decimal and one or more digit
            if match:
                quantity, _, unit = match.groups()
                quantity = float(quantity)
                unit = unit.lower()
                if unit in unit_conversion:
                    standardised_quantity = quantity * unit_conversion[unit]
                    standardised_measures.append(f"{standardised_quantity:.2f} ml")
                else:
                    standardised_measures.append(measure)  # No change if unknown unit
            else:
                standardised_measures.append(measure)  # No change if no match
        else:
            standardised_measures.append(None)

    return standardised_measures

def normalise_string(value): # Removes special char and white space char
    if value:
        return re.sub(r"[^\w\s]", "", value).strip() # Match: Not word char or white space char
    return value

# 2. Data transformation inc. standardisation and normalisation
def transform_cocktail_data(raw_data): # Custom mapping
    ingredients = [
        raw_data[f"strIngredient{i}"] for i in range(1, 16) if raw_data[f"strIngredient{i}"]
    ]
    measures = [
        raw_data[f"strMeasure{i}"] for i in range(1, 16) if raw_data[f"strMeasure{i}"]
    ]

    return {
        "CocktailID": raw_data["idDrink"],
        "Name": normalise_string(raw_data["strDrink"]),
        "Category": raw_data["strCategory"],
        "Alcoholic": raw_data["strAlcoholic"].lower() == "alcoholic",
        "GlassType": raw_data["strGlass"],
        "Ingredients": ingredients,
        "Measures": standardise_units(measures),
        "Instructions": normalise_string(raw_data["strInstructions"]),
    }

# 3. Edge cases
def handle_edge_cases(transformed_data): # Handle edge cases in the transformed data
    # Remove ingredients with no matching measurements and vice versa
    ingredients = transformed_data["Ingredients"]
    measures = transformed_data["Measures"]

    paired_data = [
        (ingredient, measure)
        for ingredient, measure in zip(ingredients, measures)
        if ingredient and measure
    ]

    transformed_data["Ingredients"] = [pair[0] for pair in paired_data]
    transformed_data["Measures"] = [pair[1] for pair in paired_data]

    return transformed_data

def main(batch_size=10):
    raw_data_batch = []
    transformed_data_batch = []

    for _ in range(batch_size):
        try:
            raw_data = fetch_cocktail_data()
            raw_data_batch.append(raw_data)

            transformed_data = transform_cocktail_data(raw_data)
            transformed_data = handle_edge_cases(transformed_data)
            transformed_data_batch.append(transformed_data)

        except Exception as e:
            print(f"Error processing cocktail: {e}")

    # Save raw and transformed if needed from batch processing.
    with open("raw_cocktail_data.json", "w") as raw_file:
        json.dump(raw_data_batch, raw_file, indent=4)

    with open("transformed_cocktail_data.json", "w") as transformed_file:
        json.dump(transformed_data_batch, transformed_file, indent=4)

    print("Batch processing complete. Data saved to files.")

if __name__ == "__main__":
    main()
