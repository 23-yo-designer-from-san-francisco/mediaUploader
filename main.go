package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/chai2010/webp"
	"github.com/dhowden/tag"
	"github.com/joe-xu/mp4parser"
	_ "github.com/lib/pq"
	uuid "github.com/satori/go.uuid"
	"github.com/sunshineplan/imgconv"
	"image"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
)

const (
	MusicStorage = "./tracks/"
	ArtworkStorage = "./artworks/"
	Extension = ".m4a"
)

var imageSizes = []int{96, 128, 192, 256, 384, 512}

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
	err := filepath.WalkDir(root, func(s string, d fs.DirEntry, e error) error {
		if e != nil { return e }
		if filepath.Ext(d.Name()) == ext {
			a = append(a, s)
		}
		return nil
	})
	if err != nil {
		return nil
	}
	log.Println(len(a))
	return a
}

func createWebpImages(src image.Image, filename string) {
	for _, value := range imageSizes {
		artwork := imgconv.Resize(src, imgconv.ResizeOption{Width: value, Height: value})
		strconv.Itoa(value)
		out, err := os.Create(ArtworkStorage + filename + "_" + strconv.Itoa(value) + "px.webp")
		if err != nil {
			log.Fatal(err.Error())
		}
		writer := io.Writer(out)
		err = webp.Encode(writer, artwork, &webp.Options{Quality: 85})
		if err != nil {
			log.Fatal(err.Error())
		}
	}
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
		err := db.QueryRow(`INSERT INTO artists(name) VALUES($1) RETURNING id`, m.Artist()).Scan(&artistID)
		if err != nil {
			return
		}
	}

	// Нашли исполнителя, теперь проверяем наличие альбома у него
	var albumID int64
	err := db.QueryRow(`SELECT id FROM albums WHERE title=$1 AND artist=$2`, m.Album(), artistID).Scan(&albumID)
	if err != nil {
		return
	}
	if albumID == 0 {
		reader := bytes.NewReader(m.Picture().Data)
		src, err := imgconv.Decode(reader)
		if err != nil {
			log.Fatal(err.Error())
		}
		artworkFilename := uuid.NewV4().String()

		// Создаем изображения webp
		createWebpImages(src, artworkFilename)

		err = db.QueryRow(`INSERT INTO albums(title, year, artist, artwork, track_count)
		 								VALUES($1, $2, $3, $4, $5) RETURNING id`, m.Album(), m.Year(), artistID, artworkFilename, totalTracks,
		).Scan(&albumID)
		if err != nil {
			return
		}
	}

	// Нашли альбом, проверяем наличие трека в нем
	var trackID int64
	err = db.QueryRow(`SELECT id FROM tracks WHERE artist=$1 AND album=$2 AND title=$3`, artistID, albumID, m.Title()).Scan(&trackID)
	if err != nil {
		return
	}
	if trackID == 0 {
		trackFile := uuid.NewV4().String() + filepath.Ext(filename)
		genre := findGenre(db, m.Genre())
		if genre == 0 {
			genre = createGenre(db, m.Genre())
		}
		copyMusicFile(filename, MusicStorage+ trackFile)
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
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	sourceD := os.Args[1]
	log.Println(sourceD)

	for _, file := range findFiles(sourceD, Extension) {
		handleFile(db, file)
	}
}

