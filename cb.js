document.addEventListener("DOMContentLoaded", function () {
  var activeTag = "";
  var searchText = "";

  // Inject right column into Material's right sidebar (empty on index page)
  var rightCol = document.getElementById("cb-right-col");
  if (rightCol) {
    var sidebarInner = document.querySelector(".md-sidebar--secondary .md-sidebar__inner");
    if (sidebarInner) {
      sidebarInner.innerHTML = "";
      sidebarInner.appendChild(rightCol);
      rightCol.style.display = "flex";
    }
  }

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

  // Wire up filter buttons
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

  // Wire up search
  var searchEl = document.getElementById("cb-search");
  if (searchEl) {
    searchEl.addEventListener("input", function () {
      searchText = this.value.toLowerCase().trim();
      applyFilters();
    });
  }
});
