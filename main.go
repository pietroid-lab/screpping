package main

import (
	"fmt"

	"github.com/gocolly/colly"
)

// GetFixtureGames busca o conteúdo da div com id "fixture_games"
func GetFixtureGames(url string) (string, error) {
	c := colly.NewCollector()
	c.UserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
	var result string
	var err error

	c.OnHTML("div#fixture_games", func(e *colly.HTMLElement) {
		result = e.Text
		if err != nil {
			fmt.Println("Erro ao obter HTML:", err)
			return
		}
	})

	err = c.Visit(url)
	if err != nil {
		return "", err
	}
	return result, nil
}

func main() {
	url := "https://www.ogol.com.br/competicao/brasileirao"
	content, err := GetFixtureGames(url)
	if err != nil {
		fmt.Println("Erro:", err)
		return
	}
	fmt.Println("Conteúdo da div fixture_games:", content)
}
