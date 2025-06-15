package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"plagiarism-detector/src/config"
	"plagiarism-detector/src/moss"
	"plagiarism-detector/src/parser"
	// "plagiarism-detector/src/simhash"
)

func main() {
	config, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	fmt.Println("Plagiarism Detector Service Started - Local Test")

	chaptersDir := "src/local-testing/s3-chapter-downloaded"
	files, err := ioutil.ReadDir(chaptersDir)
	if err != nil {
		log.Fatalf("Failed to read directory %s: %v", chaptersDir, err)
	}

	// hashesToCompare := []simhash.Simhash{}
	fp := [][]uint32{}

	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".json") {
			filePath := filepath.Join(chaptersDir, file.Name())
			fmt.Printf("\nProcessing file: %s\n", filePath)

			chapterContent, err := os.ReadFile(filePath)
			if err != nil {
				log.Printf("WARN: Failed to read file %s: %v", filePath, err)
				continue
			}

			content, err := parser.Parse(chapterContent)
			if err != nil {
				log.Printf("WARN: Failed to parse chapter content from file %s: %v", filePath, err)
				continue
			}

			fmt.Println(content)

			// hash := simhash.New(content, "HINDI")
			// fmt.Println(hash)
			// hashesToCompare = append(hashesToCompare, hash)

			fpCalculation := moss.GenerateFingerprint(content, config.MossKGramSize, config.MossWindowSize)
			fp = append(fp, fpCalculation)
		}
	}

	// hammingDistance := simhash.HammingDistance(hashesToCompare[0], hashesToCompare[1])
	// fmt.Println(hammingDistance)

	fp1 := moss.GenerateFingerprint("Raj is a good boy, And he loves going to gym and eat healthy food", config.MossKGramSize, config.MossWindowSize)
	fp2 := moss.GenerateFingerprint("Ajay is a bad boy, And he loves going to gym and bad healthy food", config.MossKGramSize, config.MossWindowSize)

	// similarity := moss.CalculateSimilarity(fp[0], fp[1])
	similarity := moss.CalculateSimilarity(fp1, fp2)
	fmt.Printf("Similarity between first two files: %.2f\n", similarity)
	if similarity >= config.MossSimilarityThreshold {
		fmt.Printf("Plagiarism detected! Similarity: %.2f (Threshold: %.2f)\n", similarity, config.MossSimilarityThreshold)
	} else {
		fmt.Printf("No plagiarism detected. Similarity: %.2f (Threshold: %.2f)\n", similarity, config.MossSimilarityThreshold)
	}
}
