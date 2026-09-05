#Testing
```python
pip install requests cryptography rich
pip install -e .          # installs the `apiyt` command
apiyt search mehrama
apiyt download <id> --out .
apiyt stream <id> | mpv -
apiyt queue add <id> && apiyt queue run
```
##Todos
- Autoplay
- Playlist support
- direct `play` 
