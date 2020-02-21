package com.github.palindromicity.vfs2.tests;

import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.test.FileInfo;

public class FileOrFolderInfo extends FileInfo {

    public FileOrFolderInfo(String name, FileType type) {
        super(name, type);
    }

    public FileOrFolderInfo(String name, FileType type, String content) {
        super(name, type, content);
    }

    public FileOrFolderInfo addFileOrFolder(String baseName) {
        final FileOrFolderInfo child = new FileOrFolderInfo(baseName, FileType.FILE_OR_FOLDER);
        addChild(child);
        return child;
    }

    public FileOrFolderInfo addFileOrFolder(String baseName, String content) {
        final FileOrFolderInfo child = new FileOrFolderInfo(baseName, FileType.FILE_OR_FOLDER, content);
        addChild(child);
        return child;
    }
}
