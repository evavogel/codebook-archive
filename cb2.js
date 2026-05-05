document.addEventListener("DOMContentLoaded", function () {
  var activeTag = "";
  var searchText = "";

  // Position the right column boxes immediately after the content area's
  // right edge. Measures the actual rendered position so it never overlaps.
  function positionRightCol() {
    var rightCol = document.getElementById("cb-right-col");
    if (!rightCol) return;

    var table = document.getElementById("cb-table");
    if (!table) return;

    var boxWidth = 480;
    var rightMargin = 20;
    var gap = 32;

    var tableRight = Math.round(table.getBoundingClientRect().right);
    var availableRight = window.innerWidth - tableRight - gap - rightMargin;

    if (availableRight < boxWidth) {
      rightCol.style.display = "none";
      return;
    }

    // Pin to the right edge of the viewport
    rightCol.style.position = "fixed";
    rightCol.style.right = rightMargin + "px";
    rightCol.style.left = "auto";
    rightCol.style.top = "4.5rem";
    rightCol.style.width = boxWidth + "px";
    rightCol.style.display = "flex";
  }

  positionRightCol();
  window.addEventListener("resize", positionRightCol);

  function getTable() {
    return document.getElementById("cb-table");
  }

  function applyFilters() {
    var table = getTable();
    if (!table) return;
    var rows = table.querySelectorAll("tbody tr");
    var visible = 0;
    rows.forEach(function (row) {
      var cells = row.cells;
      if (!cells.length) return;
      var tagsCell = cells[cells.length - 1];
      var titleCell = cells[0];
      var authorsCell = cells.length > 1 ? cells[1] : null;
      var tags = tagsCell ? tagsCell.textContent : "";
      var title = titleCell ? titleCell.textContent.toLowerCase() : "";
      var authors = authorsCell ? authorsCell.textContent.toLowerCase() : "";

      var matchTag = !activeTag || tags.indexOf(activeTag) !== -1;
      var matchSearch =
        !searchText ||
        title.indexOf(searchText) !== -1 ||
        authors.indexOf(searchText) !== -1 ||
        tags.toLowerCase().indexOf(searchText) !== -1;

      var show = matchTag && matchSearch;
      row.style.display = show ? "" : "none";
      if (show) visible++;
    });

    var label = document.getElementById("cb-count-label");
    if (label) {
      label.textContent =
        visible + " of " + rows.length + " codebooks shown";
    }
  }

  document.querySelectorAll(".cb-tag").forEach(function (btn) {
    btn.addEventListener("click", function () {
      document.querySelectorAll(".cb-tag").forEach(function (b) {
        b.classList.remove("active");
      });
      this.classList.add("active");
      activeTag = this.dataset.tag || "";
      applyFilters();
    });
  });

  var searchEl = document.getElementById("cb-search");
  if (searchEl) {
    searchEl.addEventListener("input", function () {
      searchText = this.value.toLowerCase().trim();
      applyFilters();
    });
  }
});
