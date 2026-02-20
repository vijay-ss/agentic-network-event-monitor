#!/bin/sh

MODEL="gemma3:4b"

echo "Starting Ollama server..."
ollama serve &

check_ollama_ready() {
    # Try a lightweight command to see if the server responds
    ollama list >/dev/null 2>&1
}

echo "Waiting for Ollama server to be ready..."
TIMEOUT=60
COUNTER=0

while ! check_ollama_ready; do
    sleep 2
    COUNTER=$((COUNTER + 2))
    if [ $COUNTER -ge $TIMEOUT ]; then
        echo "Ollama did not start within $TIMEOUT seconds. Exiting."
        exit 1
    fi
done

echo "Ollama is ready!"

echo "Downloading $MODEL model..."
ollama pull $MODEL

echo "Ollama is running and $MODEL model is downloaded."
wait