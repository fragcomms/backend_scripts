# Use the official Node.js 24.10.0 image as the base image
FROM node:24.10.0

# Set the working directory inside the container
WORKDIR /app

# Copy package.json and package-lock.json (if available)
COPY package*.json ./

# Install project dependencies
RUN npm install

# Copy the rest of the application code to the working directory
COPY . . 

# Set port environment variable
ENV PORT=3000
# Expose the application port
EXPOSE 3000

# Start the application
CMD ["npm", "start"]
