package parser

import (
	"bytes"
	"encoding/json"
	"golang.org/x/net/html"
	"regexp"
	"strings"
)

type Chapter struct {
	Pages []Page `json:"pages"`
}

type Page struct {
	Pagelets []Pagelet `json:"pagelets"`
}

type Pagelet struct {
	Type string `json:"type"`
	Data string `json:"data"`
}

var (
	brRegex              = regexp.MustCompile(`(?i)<(br|p)\s*/?>`)
	multipleSpacesRegex  = regexp.MustCompile(`\s+`)
	specialCharsRegex    = regexp.MustCompile(`&#\d+;`)
	nonBreakingSpaceRegex = regexp.MustCompile(`&nbsp;`)
)

func cleanHTML(htmlBody string) string {
	doc, err := html.Parse(strings.NewReader(htmlBody))
	if err != nil {
		return htmlBody
	}

	var buf bytes.Buffer
	var extractText func(*html.Node)
	extractText = func(n *html.Node) {
		if n.Type == html.TextNode {
			buf.WriteString(n.Data)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			extractText(c)
		}
	}

	extractText(doc)
	return buf.String()
}

func preProcessText(text string) string {
	processedText := brRegex.ReplaceAllString(text, "\n")
	processedText = cleanHTML(processedText)
	processedText = html.UnescapeString(processedText)
	processedText = specialCharsRegex.ReplaceAllString(processedText, "")
	processedText = nonBreakingSpaceRegex.ReplaceAllString(processedText, " ")
	processedText = multipleSpacesRegex.ReplaceAllString(processedText, " ")

	return strings.TrimSpace(processedText)
}

func Parse(chapterContent []byte) (string, error) {
	var chapter Chapter
	if err := json.Unmarshal(chapterContent, &chapter); err != nil {
		return preProcessText(string(chapterContent)), nil
	}

	var contentBuilder strings.Builder
	for _, page := range chapter.Pages {
		for _, pagelet := range page.Pagelets {
			if strings.ToUpper(pagelet.Type) == "HTML" || strings.ToUpper(pagelet.Type) == "TEXT" {
				if pagelet.Data != "" {
					cleanedText := preProcessText(pagelet.Data)
					if cleanedText != "" {
						contentBuilder.WriteString(cleanedText)
						contentBuilder.WriteString(" ")
					}
				}
			}
		}
	}

	return strings.TrimSpace(contentBuilder.String()), nil
}
