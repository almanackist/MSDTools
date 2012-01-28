import os
import sys
import time
import glob
import datetime
import sqlite3
import numpy as np

msd_subset_path='/Users/alanj/Downloads/MillionSongSubset 2'
msd_code_path='/Users/alanj/software/lib/python/MSongsPythonSrc/'
sql_code_path='/Users/alanj/software/MSongsDB/'
sys.path.append(msd_code_path)

import hdf5_getters as GETTERS

conn = sqlite3.connect(os.path.join(sql_code_path,'Tasks_Demos/SQLite/track_metadata.db'))

# display schema for a single table
def get_schema(table):
	q = conn.execute("PRAGMA table_info(["+table+"])")
	schema = q.fetchall()
	for i in schema:
		print i[0],"\t",i[1]

# for an MSD song_id, identify a song
def song_identify(songid):
	q = conn.execute("select artist_name, title, release from songs where song_id='"+songid+"'")
	songs = q.fetchall()
	print songs

# for an artist (string), display all song_ids with titles and album (release) names	    
def artist_songlist(artist):
	q = conn.execute("select song_id, title, release from songs where artist_name='"+artist+"' order by release")
	songs = q.fetchall()
	for i in songs:
		print i[0],"\t",i[1],"."*(50-len(i[1])),i[2]

# for an album release (string), display all song_ids with artist name, titles	    
def release_songlist(release):
	q = conn.execute("select song_id, artist_name, title from songs where release='"+release+"' order by release")
	songs = q.fetchall()
	for i in songs:
		print i[0],"\t",i[1],"."*(20-len(i[1])),i[2]

# for an artist (string), display total counts for songs and unique albums 
def artist_songcount_print(artist):
	q = conn.execute("select song_id, release from songs where artist_name='"+artist+"' order by release")
	songs = q.fetchall()
	print artist, "."*(30-len(artist)),
	print "Songs: ",len(songs),
	print "\tAlbums: ",
	try:
		print len(set(np.array(songs)[:,1]))
	except IndexError:
		print "None"

# for an artist (string), return a list of [artist, song count, album count]
def artist_songcount_list(artist):
	songcount_list = []
	try:
		q = conn.execute("select song_id, release from songs where artist_name='"+artist+"' order by release")
		songs = q.fetchall()
		songcount_list.append(artist)
		songcount_list.append(len(songs))
		try:
			songcount_list.append(len(set(np.array(songs)[:,1])))
		except IndexError:
			songcount_list.append(0)
	except:
		songcount_list.append(artist)
		songcount_list.append("Error")
	finally:
		return songcount_list

# for an artist (string), return a list of [artist, song count, album count] and save to csv		
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

# get list of filenames, artist, song title for all h5 files in an MSD sample directory
def MSD_sample_dirlist(path):
	dirpath = path
	dirlist = os.listdir(path)
	dirdata = []
	for fname in dirlist:
			h5 = GETTERS.open_h5_file_read(dirpath+fname)
			dirdata.append([fname, GETTERS.get_artist_name(h5),GETTERS.get_title(h5)])
                        h5.close()
        return dirdata

# get list of filenames, artist, song title for all h5 files in an MSD sample directory and save to csv
def MSD_sample_dirlist_save(path,file_path):
	import csv
	dirpath = path
	dirlist = os.listdir(path)
	dirdata = []
	for fname in dirlist:
			h5 = GETTERS.open_h5_file_read(dirpath+fname)
			dirdata.append([dirpath, fname, GETTERS.get_artist_name(h5),GETTERS.get_title(h5)])
                        h5.close()
	listwriter = csv.writer(open(file_path,'a'), delimiter=',',quotechar='|',quoting=csv.QUOTE_MINIMAL)
	listwriter.writerows(dirdata)
	return dirdata
	

