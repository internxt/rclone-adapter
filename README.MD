# rclone-adapter

Go client for Internxt Drive used by the [rclone](https://github.com/rclone/rclone) `internxt` backend.

It is not a general-purpose SDK: it only covers what the rclone backend needs (login, listing, upload and download with client-side encryption, file/folder operations and account usage).

Originally forked from [StarHack/go-internxt-drive](https://github.com/StarHack/go-internxt-drive).

## Installation

```bash
go get github.com/internxt/rclone-adapter@latest
```

Requires Go 1.25 or newer.

## Testing

```bash
go vet ./...
go test -race ./...
```

Tests run against mock servers and do not need an Internxt account.

## Releases

Versions follow [semver](https://semver.org) and are published as [GitHub releases](https://github.com/internxt/rclone-adapter/releases). While on `v0.x`, minor versions may include breaking API changes.

## Issues and support

- Problems with rclone itself or its `internxt` backend (flags, config, sync behaviour, etc.) belong in the [rclone issue tracker](https://github.com/rclone/rclone/issues).
- For bugs in this library or anything related to your Internxt account, contact [hello@internxt.com](mailto:hello@internxt.com).

## License

[MIT](LICENSE)
