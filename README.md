# youtube_data_analysis
## AIM
The idea was to create an end-to-end automated data pipeline which can be used for analysis and answer common questions such as what the most viewed channels among the 3 regions, the most liked and disliked videos,average comments on the videos and many more.

## Worflow
The workflow is divided into 3 main parts ETL(Extract, Load, Transform):
<ol>
<li><b>Extract</b>: The dataset is extracted from Kaggle Website and stored in a folder.</li>

<li><b>Transform</b>: Here the dataset is divided into 3 continents Asia,Europe,North America (which act as the dimension table).
According to the country's location the data is appended to it's respective continent.Then the dataset is cleaned(handling null values, getting rid of unwanted values, making sure the dataset has uniform datatype,and many more) for analysis.After creating dimension table, fact table is created which acts as a bridge table to join all the dimension tables(using surrogate key) along with surrogate key new columns are added:
<ul>
<li>eu_video_interaction_rate</li>
<li>na_video_interaction_rate</li> 
<li>as_video_interaction_rate</li>
</ul>
These columns give an overview about the most interacted videos in each region using views,dislikes and likes rates.</li>

<li><b>Load</b>: The cleaned datasets are then loaded into snowflake(virtual data warehouse) using snowflake connector for python. For connection and authentication I used Key Pair Authentication & Key Pair Rotation provided by snowflake. You can read more about it <a href='https://docs.snowflake.com/en/user-guide/key-pair-auth.html'>here</a>.</li>
</ol>

<p>Once data reaches snowflake, a dashboard is created using snowsight for data analysis and visualisation. The data is analysed using SQL queries.</p>
<p> This entire process is automated using Prefect and scheduled to occur everyday.</p>

## Snapshot of Dashboard

![image](https://user-images.githubusercontent.com/53776611/182353937-4ff61eba-bed9-4761-bbac-49f8f7433057.png)
