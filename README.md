# Converting HBCD redcap to reproschema

This repository contains the version-controlled implementation of the HEALthy Brain and Child Development Study (HBCD) ReproSchema, converted from REDCap data dictionaries, using the [`redcap2reproschema`](https://github.com/ReproNim/reproschema-py#redcap2reproschema-usage) tool

## Conversion Process

For each REDCap data dictionary version, we did the following

1. Removal of Previous Elements

All existing protocols, activities, and items from the previous version were deleted to ensure a clean starting point for the new version. This step was critical to clearly document which elements were removed between versions

2. Convert to reproschema format

Install the tool `reproschema-py` 
```
git clone https://github.com/ReproNim/reproschema-py.git
cd reproschema-py
pip install -e .
```
Convert the data dictionary
```
reproschema redcap2reproschema [hbcd-redcap-data-dictionary.csv] [redcap2rs.ymal]
```

3. Git add and commit

Add and commit the newly converted reproschema. For example:
```
git add .
git commit -m "converted hbcd redcap data dictionary 20230706 to reproschema"
```

4. Version tagging

After committing the changes, each version was tagged to mark the release point in the repository's history. For example:
```
git tag -a 2023.07.06 -m "redcap data dictionary 20230706 to reproschema"
```

5. Git push

Push the current tag to remote
```
# push the tag
git push origin <tag_name>

# push the content to main
git push 

# if there is a conflict, you can use --force
git push -f
```
