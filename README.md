# Setup Instructions

`uv sync`

`echo "FACTCHECK_API_KEY=YOUR_API_KEY" >> .env` # replace `YOUR_API_KEY` with
your real API key, see below for instructions your actual API key

`uv run main.py`

## 🔑 How to Get an API Key for the Google Fact Check Tools API

The Google Fact Check Tools API uses a standard Google Cloud API Key for
authentication. Here are the instructions to generate one using the Google Cloud
Console.

### Step 1: Access the Google Cloud Console

1. Go to the **Google Cloud Console** (You will need a Google account).
2. **Select or Create a Project**:
   - Click the project drop-down menu at the top of the page.
   - Select an existing Google Cloud project, or click **New Project** to create
     one specifically for the Fact Check Tools API.

### Step 2: Create the API Key

1. In the left-hand navigation menu, go to **APIs & Services** and then select
   **Credentials**.
2. Click the **+ CREATE CREDENTIALS** button at the top.
3. From the drop-down menu, select **API key**.
4. A dialog will appear displaying your newly created API key. **Copy this key
   immediately** and store it in a secure location, as it is used to
   authenticate your application's requests.

### Step 3: Enable the Fact Check Tools API

After creating the key, you must enable the specific API you wish to use on your
project.

1. In the left-hand navigation menu, go to **APIs & Services** and then select
   **Library**.
2. In the search bar, type "**Fact Check Tools API**" and press Enter.
3. Click on the **Fact Check Tools API** result.
4. On the API's page, click the **ENABLE** button.

Once enabled, your API key will be authorized to access the Fact Check Tools
API, allowing you to use its features like searching for fact-checked claims.
