// Repository page JavaScript

function toJsTree(node, isRoot = true) {
    /* Sort children: directories/folders first, then files, both alphabetically */
    let children = (node.children || []).slice();
    children.sort(function(a, b) {
        /* Directories/folders before files */
        if ((a.type === "directory" || a.type === "folder") && b.type === "file") return -1;
        if (a.type === "file" && (b.type === "directory" || b.type === "folder")) return 1;
        /* Alphabetical order (case-insensitive) */ 
        return a.name.localeCompare(b.name, undefined, {sensitivity: 'base'});
    });

    /* For S3 paths, extract only the last part of the path */
    let displayName = node.name;
    if (!isRoot && node.name && node.name.includes('/')) {
        const parts = node.name.split('/');
        displayName = parts[parts.length - 1] || parts[parts.length - 2];
    }

    return {
        data: { fullPath: node.name },
        text: displayName,
        children: children.map(function(child) { return toJsTree(child, false); }),
        icon: node.type === "file" ? "jstree-file" : "jstree-folder"
    };
}

function openToDepth(tree, node, depth) {
    if (depth <= 0) return;
    const children = tree.get_node(node).children;
    children.forEach(function(child) {
        tree.open_node(child);
        openToDepth(tree, child, depth - 1);
    });
}

$(function() {
    /* Render all local trees */
    localTrees.forEach(function(treeObj, idx) {
        const treeId = "local-tree-" + idx;
        $("#local-trees").append('<div class="mb-4"><h5>' + treeObj.label + '</h5><div id="' + treeId + '"></div></div>');
        $("#" + treeId).jstree({
            'core': {
                'data': [toJsTree(treeObj.tree)]
            },
            "plugins": ["wholerow"]
        });
        $("#" + treeId).on("ready.jstree", function(e, data) {
            const tree = data.instance;
            const root = tree.get_node('#').children[0];
            tree.open_node(root);
            openToDepth(tree, root, 2);
        });
    });

    /* Render all S3 trees */
    s3Trees.forEach(function(treeObj, idx) {
        const treeId = "s3-tree-" + idx;
        $("#s3-trees").append('<div class="mb-4"><h5>' + treeObj.label + '</h5><div id="' + treeId + '"></div></div>');
        $("#" + treeId).jstree({
            'core': {
                'data': [toJsTree(treeObj.tree)]
            },
            "plugins": ["wholerow", "types"],
            "types": {
                "default": {
                    "icon": "jstree-folder"
                },
                "file": {
                    "icon": "jstree-file"
                }
            }
        });
        $("#" + treeId).on("ready.jstree", function(e, data) {
            const tree = data.instance;
            const root = tree.get_node('#').children[0];
            tree.open_node(root);
            openToDepth(tree, root, 2);
        });
    });
});