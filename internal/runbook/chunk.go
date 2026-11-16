// Package runbook turns Markdown runbooks into searchable chunks: it splits each file into
// sections, embeds every section, and stores them in runbook_chunks.
package runbook

import (
	"fmt"
	"strings"
)

// MaxChunkChars is roughly how long one chunk may get. Short enough that one chunk is about
// one idea (so its vector isn't a blur of several topics), long enough to keep a step together
// with its command.
const MaxChunkChars = 1000

// Chunk is one searchable piece of a runbook.
type Chunk struct {
	Heading string // where it is in the runbook, e.g. "Fix > Free space quickly"
	Text    string // the section's text, without its heading line
}

// EmbedText is what actually gets embedded for a chunk. A chunk like "1. Run df -h" means
// little on its own, so the runbook title and section go in front: the vector then knows
// it's about a full disk.
func EmbedText(title string, c Chunk) string {
	return fmt.Sprintf("Runbook: %s\nSection: %s\n\n%s", title, c.Heading, c.Text)
}

// Split cuts a Markdown runbook into chunks: one per section (each "##" or deeper heading),
// and a long section is cut further at blank lines. The "# " heading is the title.
// Lines inside ``` code blocks are never treated as headings or cut apart, because
// "# comment" in a bash block is not a heading.
func Split(markdown string) (title string, chunks []Chunk) {
	var (
		headings [7]string // headings[n] = the current level-n heading, e.g. headings[2] = "Fix"
		paras    []string  // finished paragraphs of the current section
		para     []string  // lines of the paragraph being read
		inFence  bool      // inside a ``` code block
	)
	endPara := func() {
		if len(para) > 0 {
			paras = append(paras, strings.Join(para, "\n"))
			para = nil
		}
	}
	endSection := func() {
		endPara()
		heading := sectionPath(headings)
		for _, text := range pack(paras, MaxChunkChars) {
			chunks = append(chunks, Chunk{Heading: heading, Text: text})
		}
		paras = nil
	}

	for _, line := range strings.Split(markdown, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") {
			inFence = !inFence
		}
		if !inFence {
			if level, text, ok := parseHeading(line); ok {
				endSection() // a new heading closes the section before it
				if level == 1 && title == "" {
					title = text
					continue
				}
				headings[level] = text
				for i := level + 1; i < len(headings); i++ {
					headings[i] = "" // "## Fix" ends the previous "### ..." under it
				}
				continue
			}
			if trimmed == "" {
				endPara() // a blank line ends a paragraph (but not inside a code block)
				continue
			}
		}
		para = append(para, line)
	}
	endSection()
	return title, chunks
}

// parseHeading recognises "## Text" (1 to 6 #, then a space) and returns its level and text.
func parseHeading(line string) (level int, text string, ok bool) {
	for level < len(line) && line[level] == '#' {
		level++
	}
	if level == 0 || level > 6 || level >= len(line) || line[level] != ' ' {
		return 0, "", false
	}
	return level, strings.TrimSpace(line[level:]), true
}

// sectionPath joins the open headings below the title: "Fix > Free space quickly".
// Text before the first "##" (the runbook's introduction) is called "Overview".
func sectionPath(headings [7]string) string {
	var parts []string
	for _, h := range headings[1:] {
		if h != "" {
			parts = append(parts, h)
		}
	}
	if len(parts) == 0 {
		return "Overview"
	}
	return strings.Join(parts, " > ")
}

// pack joins paragraphs into chunks of at most max characters. It never cuts inside a
// paragraph, so a single paragraph longer than max becomes a chunk of its own.
func pack(paras []string, max int) []string {
	var out []string
	cur := ""
	for _, p := range paras {
		if cur != "" && len(cur)+2+len(p) > max {
			out = append(out, cur)
			cur = ""
		}
		if cur == "" {
			cur = p
		} else {
			cur += "\n\n" + p
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
