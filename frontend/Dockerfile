# Use an official Node.js image as the base image
FROM node:18-alpine

# Set the working directory in the container
WORKDIR /app

# Copy package.json and package-lock.json to the working directory
COPY package*.json ./

RUN npm i vite

# Install dependencies
RUN npm install



# Copy the rest of the application code to the working directory
COPY . .

ENV NODE_OPTIONS=--openssl-legacy-provider

# Build the React app
RUN npm run build

# Expose the port the app runs on
EXPOSE 5173

# Start the application
CMD ["npm", "run", "dev", "--", "--host=0.0.0.0"]