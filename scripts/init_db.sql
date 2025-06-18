-- Active: 1749886027822@@127.0.0.1@5438@apartment_rental_db


CREATE SCHEMA IF NOT EXISTS raw;



CREATE SCHEMA IF NOT EXISTS presentation;

DROP TABLE IF EXISTS raw.apartment_attributes;
CREATE TABLE raw.apartment_attributes (
    id INT PRIMARY KEY,
    category VARCHAR(50),         -- E.g., Studio, 1BHK, 2BHK, etc.
    body TEXT,                    -- Description of the apartment
    amenities TEXT,             -- List of amenities as an array of text
    bathrooms INTEGER,            -- Number of bathrooms
    bedrooms INTEGER,             -- Number of bedrooms
    fee NUMERIC(5, 2),           -- Maintenance or additional fee
    has_photo BOOLEAN,            -- Indicates if there are photos
    pets_allowed BOOLEAN,         -- Indicates if pets are allowed
    price_display VARCHAR(50),
    price_type VARCHAR(50),
    square_feet INTEGER,          -- Size in square feet
    address TEXT,                 -- Full address
    cityname VARCHAR(100),        -- City name
    state VARCHAR(50),           -- State name
    latitude DECIMAL(9, 6),        -- Latitude with precision
    longitude DECIMAL(9, 6)       -- longitude with precision
);


COPY raw.apartment_attributes FROM '/opt/sql-data/apartment_attributes.csv' WITH DELIMITER ',' CSV HEADER;
-- COPY raw.apartment_attributes FROM 'D:\My Documents\Amalitech\Phase2\Lab1\2--Batch ETL with Glue, Step Function\2--Batch ETL with Glue, Step Function\data\apartment_attributes.csv' WITH DELIMITER ',' CSV HEADER;


DROP TABLE  IF EXISTS  raw.user_viewing;
CREATE TABLE raw.user_viewing (
    user_id INTEGER NOT NULL,                     -- Assuming user IDs are UUIDs
    apartment_id INTEGER NOT NULL,             -- Foreign key to apartment_attributes(id)
    viewed_at VARCHAR(20),-- Timestamp of viewing
    is_wishlisted BOOLEAN,       -- Whether the user wishlisted the apartment
    call_to_action VARCHAR(50),                -- E.g., Contact, Book Now, Save for Later
    PRIMARY KEY (user_id, apartment_id, viewed_at),
    FOREIGN KEY (apartment_id) REFERENCES raw.apartment_attributes(id) ON DELETE CASCADE
);


COPY raw.user_viewing FROM '/opt/sql-data/user_viewing.csv' WITH DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS  raw.apartments;
CREATE TABLE raw.apartments (
    id INTEGER PRIMARY KEY,                           -- Unique identifier
    title VARCHAR(255),                     -- Title of the listing
    source VARCHAR(50),                             -- E.g., Airbnb, Zillow
    price NUMERIC(6, 2),                   -- Apartment price
    currency VARCHAR(10),              -- Currency of the price
    listing_created_on VARCHAR(20),  -- Listing creation time
    is_active BOOLEAN,                  -- Whether listing is currently active
    last_modified_timestamp VARCHAR(20)  -- Last update time
);

COPY raw.apartments FROM '/opt/sql-data/apartments.csv' WITH DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS raw.bookings;
CREATE TABLE raw.bookings (
    booking_id INTEGER PRIMARY KEY,                      -- Unique booking identifier
    user_id INTEGER,                              -- References user_viewing (user_id)
    apartment_id INTEGER,                      -- References apartments (id)
    booking_date VARCHAR(20),      -- When the booking was made
    checkin_date VARCHAR(20),                         -- Start of the stay
    checkout_date VARCHAR(20),                        -- End of the stay
    total_price NUMERIC(7, 2),                -- Total amount paid
    currency VARCHAR(10),                 -- Currency used
    booking_status VARCHAR(50),       -- confirmed, canceled, pending     
    FOREIGN KEY (apartment_id) REFERENCES raw.apartments(id) ON DELETE CASCADE
);

COPY raw.bookings FROM '/opt/sql-data/bookings.csv' WITH DELIMITER ',' CSV HEADER;