#!/bin/bash

echo "Enter your GitHub username:"
read GH_USER

echo "Enter the repository name to use/create:"
read REPO_NAME

echo "Enter your GitHub Personal Access Token (PAT):"
read -s PAT

# Check if repo exists
echo "Checking if https://api.github.com/repos/$GH_USER/$REPO_NAME exists..."

HTTP_STATUS=$(curl -o /dev/null -s -w "%{http_code}\n" \
    -H "Authorization: token $PAT" \
    https://api.github.com/repos/$GH_USER/$REPO_NAME)

if [ "$HTTP_STATUS" -eq 200 ]; then
    echo "Repo already exists on GitHub."
else
    echo "Repo does not exist. Creating new repo: $REPO_NAME"

    CREATE_STATUS=$(curl -s -o /dev/null -w "%{http_code}\n" \
      -H "Authorization: token $PAT" \
      -d "{\"name\": \"$REPO_NAME\", \"private\": false}" \
      https://api.github.com/user/repos)

    if [ "$CREATE_STATUS" -eq 201 ]; then
        echo "Repository created successfully!"
    else
        echo "Failed to create repository! HTTP status: $CREATE_STATUS"
        exit 1
    fi
fi

# Initialize local git repo
if [ ! -d ".git" ]; then
    echo "Initializing new local git repository..."
    git init
fi

echo "Adding files..."
git add .

echo "Enter commit message:"
read COMMIT_MSG
git commit -m "$COMMIT_MSG"

# Set remote
REMOTE_URL="https://$GH_USER:$PAT@github.com/$GH_USER/$REPO_NAME.git"

git remote remove origin 2>/dev/null
git remote add origin "$REMOTE_URL"

# Push
echo "Pushing to GitHub..."
git push -u origin main 2>/dev/null || git push -u origin master

echo "Done!"

