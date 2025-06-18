import os
import asyncio
from dotenv import load_dotenv
from openai import AsyncOpenAI
from tqdm.asyncio import tqdm

# Load environment variables from .env
load_dotenv()

# Get OpenAI API key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY not found in .env file.")

# OpenAI async client setup
client = AsyncOpenAI(api_key=OPENAI_API_KEY)

# Model and limits
MODEL = "gpt-4.1-nano"
BATCH_SIZE = 50
MAX_CONCURRENT_REQUESTS = 10  # Stay well under 500 RPM (safe for retries)

INPUT_FILE = "unique_departments.txt"
OUTPUT_FILE = "parks_departments.txt"

# --- Updated prompt: ask for plain text, one match per line, no JSON, no code block ---
SYSTEM_PROMPT = (
    "You are an expert in municipal government. "
    "Given an array of department names, return ONLY those that are related to parks, recreation, or outdoor recreation. "
    "Return each matching department name on its own line, as plain text. Do not return any JSON, lists, or code blocks. If none match, return nothing."
)

def read_batches(filename, batch_size):
    """Yield batches of lines from a file."""
    batch = []
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                batch.append(line)
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

async def classify_parks_departments(batch, semaphore):
    """Send a batch to OpenAI and return the filtered list as plain text lines."""
    user_content = "\n".join(batch)
    messages = [
        {"role": "user", "content": user_content},
        {"role": "system", "content": SYSTEM_PROMPT},
    ]
    async with semaphore:
        try:
            response = await client.chat.completions.create(
                model=MODEL,
                messages=messages,
                max_tokens=500,
                temperature=0.0,
                timeout=30,
            )
            content = response.choices[0].message.content
            # Split by newlines, filter out empty lines
            lines = [line.strip() for line in content.splitlines() if line.strip()]
            return lines
        except Exception as e:
            print(f"Error with batch: {e}")
            return []

async def main():
    # Prepare batches
    batches = list(read_batches(INPUT_FILE, BATCH_SIZE))
    total_batches = len(batches)
    all_results = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    # Use tqdm for async progress bar
    async def process_and_collect(batch):
        matches = await classify_parks_departments(batch, semaphore)
        return matches

    # Schedule all tasks
    tasks = [process_and_collect(batch) for batch in batches]
    results = []
    for coro in tqdm(asyncio.as_completed(tasks), total=total_batches, desc="Processing batches (parallel)"):
        matches = await coro
        results.extend(matches)

    # Write results to file, one department per line
    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        for match in results:
            out.write(match + "\n")
    print(f"Done. {len(results)} parks/recreation departments found.")

if __name__ == "__main__":
    asyncio.run(main()) 