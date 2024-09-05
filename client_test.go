package s3utils

import (
	"testing"
	"time"
)

func Test_generateObjectKeyByDate(t *testing.T) {
	type args struct {
		destination string
		fileName    string
		date        time.Time
	}

	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "case 1",
			args: args{
				destination: "dir1",
				fileName:    "test.txt",
				date:        time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			want: "dir1/_year=2022/_month=01/_day=01/_date=2022-01-01/test.txt",
		},
		{
			name: "case 2",
			args: args{
				destination: "directory",
				fileName:    "test.json",
				date:        time.Date(2024, 9, 30, 0, 0, 0, 0, time.UTC),
			},
			want: "directory/_year=2024/_month=09/_day=30/_date=2024-09-30/test.json",
		},
		{
			name: "case 3 extended path",
			args: args{
				destination: "directory/raw",
				fileName:    "local_dir/test.json",
				date:        time.Date(2024, 9, 30, 0, 0, 0, 0, time.UTC),
			},
			want: "directory/raw/_year=2024/_month=09/_day=30/_date=2024-09-30/test.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := generateObjectKeyByDate(tt.args.destination, tt.args.fileName, tt.args.date); got != tt.want {
				t.Errorf("actual `%v` \n expected `%v`", got, tt.want)
			}
		})
	}
}

func Test_generateFolderDestinationByDate(t *testing.T) {
	type args struct {
		destination string
		date        time.Time
	}

	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "case 1",
			args: args{
				destination: "dir1",
				date:        time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			want: "dir1/_year=2022/_month=01/_day=01/_date=2022-01-01",
		},
		{
			name: "case 2",
			args: args{
				destination: "directory",
				date:        time.Date(2024, 9, 30, 0, 0, 0, 0, time.UTC),
			},
			want: "directory/_year=2024/_month=09/_day=30/_date=2024-09-30",
		},
		{
			name: "case 3 extended path",
			args: args{
				destination: "directory",
				date:        time.Date(2024, 9, 30, 0, 0, 0, 0, time.UTC),
			},
			want: "directory/_year=2024/_month=09/_day=30/_date=2024-09-30",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := generateFolderDestinationByDate(tt.args.destination, tt.args.date); got != tt.want {
				t.Errorf("actual `%v` \n expected `%v`", got, tt.want)
			}
		})
	}
}
