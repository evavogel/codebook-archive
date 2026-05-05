document.addEventListener("DOMContentLoaded", function () {
  var activeTag = "";
  var searchText = "";

  // Position the right column boxes immediately after the content area's
  // right edge. Measures the actual rendered position so it never overlaps.
  function positionRightCol() {
    var rightCol = document.getElementById("cb-right-col");
    if (!rightCol) return;

    var content = document.querySelector(".md-content__inner");
    if (!content) return;

    // Measure the actual table right edge (it may overflow the content div)
    var table = document.getElementById("cb-table");
    var tableRight = table
      ? Math.round(table.getBoundingClientRect().right)
      : Math.round(content.getBoundingClientRect().right);

    var gap = 36;
    var rightMargin = 16; // gap from right edge of viewport
    var left = tableRight + gap;
    var availableWidth = window.innerWidth - left - rightMargin;

    if (availableWidth < 180) {
      rightCol.style.display = "none";
      return;
    }

    // Use ALL available space to the right — no artificial width cap
    var width = availableWidth;

    rightCol.style.position = "fixed";
    rightCol.style.left = left + "px";
    rightCol.style.top = "4.5rem";
    rightCol.style.width = width + "px";
    rightCol.style.display = "flex";
    rightCol.style.right = "auto";
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
