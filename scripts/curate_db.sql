-- Active: 1749886027822@@127.0.0.1@5438@apartment_rental_db
CREATE SCHEMA IF NOT EXISTS curated;



CREATE TABLE IF NOT EXISTS curated.apartment_bookings (
    booking_id INTEGER PRIMARY KEY,                      -- Unique booking identifier
    apartment_id INTEGER NOT NULL,             -- Foreign key to apartment_attributes(id)
    user_id INTEGER NOT NULL,                     -- Assuming user IDs are UUIDs
    category VARCHAR(50),         -- E.g., Studio, 1BHK, 2BHK, etc.
    body TEXT,                    -- Description of the apartment
    cityname VARCHAR(100),        -- City name
    state VARCHAR(50),           -- State name
    title VARCHAR(255),                     -- Title of the listing
    source VARCHAR(50),                             -- E.g., Airbnb, Zillow
    listing_created_on DATE,  -- Listing creation time
    is_active BOOLEAN,                  -- Whether listing is currently active
    booking_date DATE,      -- When the booking was made
    checkin_date DATE,                         -- Start of the stay
    checkout_date DATE,                        -- End of the stay
    booking_status VARCHAR(50),       -- confirmed, canceled, pending     
    total_price_usd DOUBLE PRECISION       -- confirmed, canceled, pending     

);




CREATE TABLE IF NOT EXISTS presentation.occupancy_rate_per_month(
    month DATE PRIMARY KEY,
    total_bookings INT NOT NULL,
    total_booked_nights INT NOT NULL,
    occupancy_rate_percent DOUBLE PRECISION NOT NULL
)


CREATE TABLE IF NOT EXISTS presentation.top_listings_weekly_revenue(
    week DATE PRIMARY KEY,
    apartment_id INT NOT NULL,
    weekly_revenue DOUBLE PRECISION NOT NULL
)

CREATE TABLE IF NOT EXISTS presentation.popular_cities_per_week(
    week DATE,
    cityname VARCHAR(100),
    total_bookings INT NOT NULL
)


CREATE TABLE IF NOT EXISTS presentation.avg_booking_duration_per_month(
    month DATE PRIMARY KEY,
    avg_booking_duration_days  DOUBLE PRECISION NOT NULL

)

CREATE TABLE IF NOT EXISTS presentation.total_bookings_per_user(
    user_id INTEGER PRIMARY KEY,
    total_bookings INTEGER NOT NULL
)

CREATE TABLE IF NOT EXISTS presentation.repeat_customer_rate_per_month(
    month DATE PRIMARY KEY,
    num_repeated_customers INT NOT NULL,
    total_customers INT NOT NULL,
    repeat_customer_rate_percent DOUBLE PRECISION NOT NULL
)



