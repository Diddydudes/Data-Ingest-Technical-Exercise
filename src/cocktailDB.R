#!/usr/bin/env Rscript
# This script was created for a technical exercise.
# The data used is from TheCocktailDB API. This is a free to use dataset
# This script will process the json data from the api normalise and standardise the output
# and outputs both raw and transformed data to files.
# These files can be used for further analysis and reporting using the transformed data 
# or verification using the original raw data.

# Define a vector of required packages
required_packages <- c("httr", "jsonlite", "stringr")

# Function to check and install missing packages
install_if_missing <- function(packages) {
  for (pkg in packages) {
    if (!require(pkg, character.only = TRUE)) {
      install.packages(pkg, dependencies = TRUE)
    }
  }
}

# Install missing packages
install_if_missing(required_packages)

library(httr)
library(jsonlite)
library(stringr)

# 1 Data Ingestion
fetch_cocktail_data <- function() {
  url <- "https://www.thecocktaildb.com/api/json/v1/1/random.php"
  response <- GET(url)

  if (status_code(response) == 200) {
    data <- content(response, as = "parsed")
    return(data$drinks[[1]])
  } else {
    stop(sprintf("Failed to fetch data. Status code: %d", status_code(response)))
  }
}

standardise_units <- function(measures) {
  unit_conversion <- list(
    oz = 29.57,
    ml = 1.0,
    cl = 10.0,
    tsp = 4.93,
    tbsp = 14.79
  )

  standardised_measures <- sapply(measures, function(measure) {
    if (!is.null(measure)) {
      match <- str_match(measure, "(\\d+(\\.\\d+)?)\\s*(\\w+)")
      if (!is.na(match[1])) {
        quantity <- as.numeric(match[2])
        unit <- tolower(match[4])
        if (unit %in% names(unit_conversion)) {
          standardised_quantity <- quantity * unit_conversion[[unit]]
          return(sprintf("%.2f ml", standardised_quantity))
        } else {
          return(measure)  # No change if unknown unit
        }
      } else {
        return(measure)  # No change if no match
      }
    } else {
      return(NA)
    }
  })

  return(standardised_measures)
}

normalise_string <- function(value) {
  if (!is.null(value)) {
    return(trimws(gsub("[^\\w\\s]", "", value)))
  }
  return(value)
}

# 2. Data transformation inc. standardisation and normalisation
transform_cocktail_data <- function(raw_data) {
  ingredients <- unlist(lapply(1:15, function(i) raw_data[[paste0("strIngredient", i)]]))
  ingredients <- ingredients[!is.null(ingredients)]

  measures <- unlist(lapply(1:15, function(i) raw_data[[paste0("strMeasure", i)]]))
  measures <- measures[!is.null(measures)]

  return(list(
    CocktailID = raw_data$idDrink,
    Name = normalise_string(raw_data$strDrink),
    Category = raw_data$strCategory,
    Alcoholic = tolower(raw_data$strAlcoholic) == "alcoholic",
    GlassType = raw_data$strGlass,
    Ingredients = ingredients,
    Measures = standardise_units(measures),
    Instructions = normalise_string(raw_data$strInstructions)
  ))
}

# 3. Edge cases
handle_edge_cases <- function(transformed_data) {
  ingredients <- transformed_data$Ingredients
  measures <- transformed_data$Measures
  
  # Ensure both vectors are of equal length
  paired_data <- Filter(
    function(x) !is.null(x[[1]]) && !is.null(x[[2]]),
    mapply(
      list,
      head(ingredients, min(length(ingredients), length(measures))),
      head(measures, min(length(ingredients), length(measures))),
      SIMPLIFY = FALSE
    )
  )
  
  transformed_data$Ingredients <- sapply(paired_data, `[[`, 1)
  transformed_data$Measures <- sapply(paired_data, `[[`, 2)
  
  return(transformed_data)
}

main <- function(batch_size = 10) {
  raw_data_batch <- list()
  transformed_data_batch <- list()

  for (i in 1:batch_size) {
    tryCatch({
      raw_data <- fetch_cocktail_data()
      raw_data_batch <- append(raw_data_batch, list(raw_data))

      transformed_data <- transform_cocktail_data(raw_data)
      transformed_data <- handle_edge_cases(transformed_data)
      transformed_data_batch <- append(transformed_data_batch, list(transformed_data))

    }, error = function(e) {
      message(sprintf("Error processing cocktail: %s", e$message))
    })
  }

  write(toJSON(raw_data_batch, pretty = TRUE, auto_unbox = TRUE), "raw_cocktail_data.json")
  write(toJSON(transformed_data_batch, pretty = TRUE, auto_unbox = TRUE), "transformed_cocktail_data.json")

  print("Batch processing complete. Data saved to files.")
}

if (interactive()) {
  main()
}