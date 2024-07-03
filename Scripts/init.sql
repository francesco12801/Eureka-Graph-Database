CREATE TABLE IF NOT EXISTS "User" (
    userId BIGINT PRIMARY KEY,
    screenName VARCHAR(50),
    avatar VARCHAR(255),
    followersCount INT,
    followingCount INT,
    lang VARCHAR(10)
)

CREATE TABLE IF NOT EXISTS "Post" (
    postId BIGINT PRIMARY KEY,
    userId BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    FOREIGN KEY (userId) REFERENCES "User" (userId)
)

CREATE TABLE IF NOT EXISTS "Tag" (
    text VARCHAR(50) PRIMARY KEY
)

CREATE TABLE IF NOT EXISTS "FOLLOWS" (
    followerId BIGINT NOT NULL,
    followedId BIGINT NOT NULL,
    PRIMARY KEY (followerId, followedId),
    FOREIGN KEY (followerId) REFERENCES "User" (userId),
    FOREIGN KEY (followedId) REFERENCES "User" (userId)
)

CREATE TABLE IF NOT EXISTS "HAS_TAG" (
    postId BIGINT NOT NULL,
    tagText VARCHAR(50) NOT NULL,
    PRIMARY KEY (postId, tagText),
    FOREIGN KEY (postId) REFERENCES "Post" (postId),
    FOREIGN KEY (tagText) REFERENCES "Tag" (text)
)