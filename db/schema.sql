CREATE TABLE IF NOT EXISTS `Repositories` (
    `id_repo` INTEGER,
    `name` text NOT NULL,
    `stars_url` text NOT NULL,
    `forks_url` text NOT NULL,
    `commits_url` text NOT NULL,
    PRIMARY KEY (`id_repo`)
);

CREATE TABLE IF NOT EXISTS `Issues` (
    `id_issue` INTEGER,
    `id_repo` INTEGER,
    `created_at` text,
    `title` text NOT NULL,
    `comments_url` text NOT NULL,
    PRIMARY KEY (`id_issue`),
    FOREIGN KEY(`id_repo`) REFERENCES Repositories(`id_repo`)
);

CREATE TABLE IF NOT EXISTS `Comments` (
    `id_comment` INTEGER,
    `id_issue` INTEGER,
    `created_at` text,
    `text` TEXT NOT NULL,
    `is_toxic` INTEGER(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (`id_comment`),
    FOREIGN KEY(`id_issue`) REFERENCES Issues(`id_issue`)
);
