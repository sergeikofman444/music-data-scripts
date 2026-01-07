The data was assembled from this dataset by mhollingshead: https://github.com/mhollingshead/billboard-hot-100
The dataset was broken into weekly chart instances, which would then be used for analytics.
Each Track/Artist was then search via the Spotify API (using spotipy), to add the track ISRC, Spotify ID, and to pair the track with a relation of artist(s). 
Then, the data was enriched with track release date, and artist type (group/individual), artist country of origin, and artist year of origin, using the MusicBrainz API. 
This data is visualized on a web-app, here: https://www.chartanalytica.xyz/
