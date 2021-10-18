package main

import (
	"database/sql"
	"fmt"
	"github.com/dhowden/tag"
	"github.com/joe-xu/mp4parser"
	_ "github.com/lib/pq"
	"github.com/satori/go.uuid"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

const MUSIC_STORAGE = "/Volumes/ram/tracks/"
const ARTWORK_STORAGE = "/Volumes/ram/artworks/"
const EXTENSION = ".m4a"
const SOURCE_DIR = "/Users/test/ts"

type Metadata struct {
	tag.Metadata
	Duration float64
}

// Читает метаданные файла, возвращает метаданные и имя файла с обложкой
func getMetadata(filename string) Metadata {
	var meta Metadata
	f, _ := os.Open(filename)
	m, err := tag.ReadFrom(f)
	if err != nil {
		log.Fatal(err)
	}
	meta.Metadata = m
	p := mp4parser.NewParser(f)
	info, _ := p.Parse()
	meta.Duration = info.Duration().Seconds()
	fmt.Println("Album", meta.Album())
	return meta
}

func findGenre(db *sql.DB, genre string) int64 {
	var gnr int64
	if err := db.QueryRow(`SELECT id FROM genres where name=$1`, genre).Scan(&gnr); err != nil {
		log.Println(err)
	}
	return gnr
}

func createGenre(db *sql.DB, genre string) int64 {
	var gnr int64
	if err := db.QueryRow(`INSERT INTO genres(name) VALUES($1) RETURNING id`, genre).Scan(&gnr); err != nil {
		log.Println(err)
	}
	return gnr
}

func copyMusicFile(src string, dest string) {
	fmt.Println(src, dest)
	var args = []string{"-i", src, "-map", "0:a", "-c:a", "copy", "-map_metadata", "-1", dest}
	cmd := exec.Command("ffmpeg", args...)
	_, err := cmd.Output()
	if err != nil {
		log.Fatalln(err)
	}
}

func findFiles(root, ext string) []string {
	var a []string
	filepath.WalkDir(root, func(s string, d fs.DirEntry, e error) error {
		if e != nil { return e }
		if filepath.Ext(d.Name()) == ext {
			a = append(a, s)
		}
		return nil
	})
	return a
}

func handleFile(db *sql.DB, filename string) {
	m := getMetadata(filename)
	trackNumber, totalTracks := m.Track()

	// Сначала проверяем, существует ли исполнитель. Если нет, добавляем его. Если он уже есть, получаем его id
	// -> Поле name в artist должно быть unique
	var artistID int64
	if err := db.QueryRow(`SELECT id FROM artists
			WHERE name=$1`, m.Artist()).Scan(&artistID); err != nil {
		log.Println(err)  // no rows in result set, если исполнителя нет
	}

	if artistID == 0 {
		db.QueryRow(`INSERT INTO artists(name) VALUES($1) RETURNING id`, m.Artist()).Scan(&artistID)
		fmt.Println("added artist", artistID)
	}

	// Нашли исполнителя, теперь проверяем наличие альбома у него
	var albumID int64
	db.QueryRow(`SELECT id FROM albums WHERE title=$1 AND artist=$2`, m.Album(), artistID).Scan(&albumID)
	if albumID == 0 {
		artName := uuid.NewV4().String() + "." + m.Picture().Ext
		artwork, _ := os.Create(ARTWORK_STORAGE + artName)
		defer artwork.Close()
		artwork.Write(m.Picture().Data)
		db.QueryRow(`INSERT INTO albums(title, year, artist, artwork, track_count)
 								VALUES($1, $2, $3, $4, $5) RETURNING id`, m.Album(), m.Year(), artistID, artName, totalTracks,
		).Scan(&albumID)
	}

	// Нашли альбом, проверяем наличие трека в нем
	var trackID int64
	db.QueryRow(`SELECT id FROM tracks WHERE artist=$1 AND album=$2 AND name=$3`, artistID, albumID, m.Title()).Scan(&trackID)
	if trackID == 0 {
		trackFile := uuid.NewV4().String() + filepath.Ext(filename)
		genre := findGenre(db, m.Genre())
		if genre == 0 {
			genre = createGenre(db, m.Genre())
		}
		copyMusicFile(filename, MUSIC_STORAGE + trackFile)
		err := db.QueryRow(`INSERT INTO tracks(title, artist, album, genre, number, file, duration) VALUES($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
			m.Title(), artistID, albumID, genre, trackNumber, trackFile, m.Duration).Scan(&trackID)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("%s by %s added", m.Title(), m.Artist())
	}
}

func main() {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		os.Getenv("DBUSER"), os.Getenv("DBPASS"), os.Getenv("DBHOST"), os.Getenv("DBPORT"),
		os.Getenv("DBNAME"))
	fmt.Println(connStr)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range findFiles(SOURCE_DIR, EXTENSION) {
		handleFile(db, file)
	}
}
