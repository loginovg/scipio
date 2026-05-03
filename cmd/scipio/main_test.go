package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldReturnSortedSqlFilesWhenMigrationPathIsDirectory(t *testing.T) {
	t.Parallel()

	// given
	tempDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "a"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(tempDir, "b"), 0o755))

	firstFile := filepath.Join(tempDir, "a", "V001_init.sql")
	secondFile := filepath.Join(tempDir, "b", "V002_next.sql")
	ignoredFile := filepath.Join(tempDir, "b", "README.txt")

	require.NoError(t, os.WriteFile(firstFile, []byte("SELECT 1;"), 0o644))
	require.NoError(t, os.WriteFile(secondFile, []byte("SELECT 2;"), 0o644))
	require.NoError(t, os.WriteFile(ignoredFile, []byte("ignore"), 0o644))

	// when
	files, err := loadMigrationFiles(tempDir)

	// then
	require.NoError(t, err)
	require.Equal(t, []string{firstFile, secondFile}, files)
}

func TestShouldReturnSingleFileWhenMigrationPathIsSqlFile(t *testing.T) {
	t.Parallel()

	// given
	tempDir := t.TempDir()
	migrationFile := filepath.Join(tempDir, "V001_init.sql")
	require.NoError(t, os.WriteFile(migrationFile, []byte("SELECT 1;"), 0o644))

	// when
	files, err := loadMigrationFiles(migrationFile)

	// then
	require.NoError(t, err)
	require.Equal(t, []string{migrationFile}, files)
}

func TestShouldReturnErrorWhenMigrationDirectoryHasNoSqlFiles(t *testing.T) {
	t.Parallel()

	// given
	tempDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "note.txt"), []byte("x"), 0o644))

	// when
	files, err := loadMigrationFiles(tempDir)

	// then
	require.Error(t, err)
	require.Nil(t, files)
}
