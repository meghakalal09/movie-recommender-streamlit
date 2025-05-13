import streamlit as st
from confluent_kafka import Producer, Consumer
import json
import time
from kafka_config import conf
import pandas as pd
from PIL import Image

# ----------------------------
# Streamlit UI Setup
# ----------------------------
st.set_page_config(page_title="🎬 Movie Recommender", layout="centered")
st.markdown("""
    <style>
        .main .block-container {
            max-width: 70% !important;
            padding-left: 2rem;
            padding-right: 2rem;
        }
    </style>
    """, unsafe_allow_html=True)
st.title("🎬 Rate Movies and Get Recommendations")

# Load movie metadata to be rated
movies_to_rate_file = pd.read_csv('streamlit_assets/movies_to_rate.csv')
movies_to_rate = movies_to_rate_file.to_dict(orient='records')

user_id = st.text_input("Enter your user ID:")
st.subheader("Rate the following movies (1-5):")

#User rating form
user_ratings = {}
with st.form("rating_form"):
    for i in range(0, len(movies_to_rate), 2):
        cols = st.columns(2)

        for j in range(2):
            if i + j < len(movies_to_rate):
                row = movies_to_rate[i + j]
                movie_id = row["movieId"]
                movie_title = row["title"]
                image_path = f"streamlit_assets/{movie_id}.jpg"

                with cols[j]:
                    inner_col1, inner_col2 = st.columns([3, 5])

                    with inner_col1:
                        try:
                            st.image(image_path, width=120)
                        except:
                            st.warning("❗No image found")

                    with inner_col2:
                        st.markdown(f"**{movie_title}**")
                        rating = st.radio(
                            label="",
                            options=[1, 2, 3, 4, 5],
                            index=2,
                            key=str(movie_id),
                            horizontal=True
                        )
                        user_ratings[movie_id] = rating

    submitted = st.form_submit_button("Submit Ratings")



# ----------------------------
# Kafka Producer Setup
# ----------------------------
producer = Producer(conf)

# ----------------------------
# Handle Form Submission & Await Recommendations
# ----------------------------
if submitted:
    if not user_id.strip():
        st.error("❗ Please enter a valid user ID.")
    else:
        st.success("✅ Ratings submitted successfully!")

        # Build Kafka payload
        ratings_payload = {
            "userId": user_id,
            "ratings": [
                {"movieId": int(movie_id), "rating": float(rating)}
                for movie_id, rating in user_ratings.items()
            ]
        }

        # Send rating message to Kafka input topic
        producer.produce(
            topic="movie-recommendation-input",
            key=user_id,
            value=json.dumps(ratings_payload)
        )
        producer.flush()

        # Setup Kafka consumer to receive output
        consumer_conf = conf.copy()
        consumer_conf['group.id'] = f"streamlit-ui-{user_id}"
        consumer = Consumer(consumer_conf)
        consumer.subscribe(["movie-recommendation-output"])

        st.info("📡 Waiting for recommendations...")

        # Poll for response
        recommendations = []
        timeout = 55
        start = time.time()

        while time.time() - start < timeout:
            msg = consumer.poll(1.0)
            if msg and not msg.error():
                data = json.loads(msg.value().decode())
                print(f"📩 Received from Kafka: {data}")  # debug print
                if data.get("userId") == user_id:
                    recommendations = data.get("recommended_movies", [])
                    break

        consumer.close()

        # Display recommendations
        if recommendations:
            st.subheader("🍿 Your Recommended Movies:")
            for movie in recommendations:
                st.markdown(f"\u2022 **{movie}**")
        else:
            st.warning("⚠️ No recommendations received yet. Please try again shortly.")