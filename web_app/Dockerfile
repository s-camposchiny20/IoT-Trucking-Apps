# Use an official Python runtime as a parent image
FROM python:3.6

# Download sasl modules for PyHive
RUN apt-get update && apt-get install -y libsasl2-modules libsasl2-dev

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY app/ /app

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Make port 8501 available to the world outside this container
EXPOSE 8501

# Run app.py when the container launches
CMD ["streamlit", "run", "app.py"]