import csv
import os
import sys
import time
import glob
import datetime
import sqlite3
msd_subset_path='/Users/alanj/Downloads/MillionSongSubset 2'
msd_code_path='/Users/alanj/software/lib/python/MSongsPythonSrc/'
sql_code_path='/Users/alanj/software/MSongsDB/'
sys.path.append(msd_code_path)


conn = sqlite3.connect(os.path.join(sql_code_path,'Tasks_Demos/SQLite/track_metadata.db'))



WAartists = csv.reader(open('/Users/alanj/Desktop/WAartists.csv'))

songcount_list = []

def artistlist_songcount(artist_list,file_path):
    import csv
    songcount_list = []
    for name in artist_list:
        artist_songcount = [] 
        try:
            q = conn.execute("select song_id, release from songs where artist_name='"+name+"' order by release")
            songs = q.fetchall()
            artist_songcount.append(name)
            artist_songcount.append(len(songs))
            try:
                artist_songcount.append(len(set(np.array(songs)[:,1])))
            except IndexError:
                artist_songcount.append(0)
        except:
            artist_songcount.append(name)
            artist_songcount.append("Error")
        finally:
            songcount_list.append(artist_songcount)
    songwriter = csv.writer(open(file_path,'a'), delimiter=',',quotechar='|',quoting=csv.QUOTE_MINIMAL)
    songwriter.writerows(songcount_list)
    return songcount_list
