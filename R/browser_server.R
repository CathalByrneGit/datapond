# R/browser_server.R
# Shiny module server for data lake browser

#' @title Shiny module server for db_browser()
#' @export
#' @family shiny
#' @description Use [db_browser_ui()] and `db_browser_server()`
#'   to include the data lake browser as a Shiny module in your own app.
#' @param id Character of length 1, module ID (must match UI)
#' @param height Height of data preview tables
#' @return A Shiny module server function
#' @examples
#' \dontrun{
#' # In your Shiny app:
#' server <- function(input, output, session) {
#'   db_browser_server("browser1")
#' }
#' }
db_browser_server <- function(id, height = "500px") {
  
  shiny::moduleServer(id, function(input, output, session) {
    
    ns <- session$ns
    
    # Reactive values
    rv <- shiny::reactiveValues(
      selected_section = NULL,
      selected_dataset = NULL,
      selected_schema = NULL,
      selected_table = NULL,
      tree_refresh = 0
    )
    
    # Get mode
    curr_mode <- .db_get("mode")
    is_hive <- identical(curr_mode, "hive")
    
    # =========================================================================
    # Tree View
    # =========================================================================
    
    output$tree_view <- shiny::renderUI({
      # Trigger refresh
      rv$tree_refresh
      
      if (is_hive) {
        .render_hive_tree(ns, rv)
      } else {
        .render_ducklake_tree(ns, rv)
      }
    })
    
    # Refresh button
    shiny::observeEvent(input$refresh_tree, {
      rv$tree_refresh <- rv$tree_refresh + 1
    })
    
    # Handle tree selection (hive mode)
    shiny::observeEvent(input$tree_selection, {
      sel <- input$tree_selection
      if (!is.null(sel) && grepl("/", sel)) {
        parts <- strsplit(sel, "/")[[1]]
        rv$selected_section <- parts[1]
        rv$selected_dataset <- parts[2]
      }
    })
    
    # Handle tree selection (DuckLake mode)
    shiny::observeEvent(input$tree_selection_lake, {
      sel <- input$tree_selection_lake
      if (!is.null(sel) && grepl("\\.", sel)) {
        parts <- strsplit(sel, "\\.")[[1]]
        rv$selected_schema <- parts[1]
        rv$selected_table <- parts[2]
      }
    })
    
    # =========================================================================
    # Selected Info
    # =========================================================================
    
    output$selected_info <- shiny::renderUI({
      if (is_hive) {
        if (is.null(rv$selected_section) || is.null(rv$selected_dataset)) {
          return(shiny::tags$em("Select a dataset from the tree on the left"))
        }
        shiny::tags$div(
          shiny::tags$strong("Selected: "),
          shiny::tags$code(paste0(rv$selected_section, "/", rv$selected_dataset))
        )
      } else {
        if (is.null(rv$selected_schema) || is.null(rv$selected_table)) {
          return(shiny::tags$em("Select a table from the tree on the left"))
        }
        shiny::tags$div(
          shiny::tags$strong("Selected: "),
          shiny::tags$code(paste0(rv$selected_schema, ".", rv$selected_table))
        )
      }
    })
    
    # =========================================================================
    # Preview Tab
    # =========================================================================
    
    preview_data <- shiny::eventReactive(input$load_preview, {
      n_rows <- input$preview_rows %||% 100
      
      tryCatch({
        if (is_hive) {
          shiny::req(rv$selected_section, rv$selected_dataset)
          data <- db_hive_read(rv$selected_section, rv$selected_dataset)
          head(dplyr::collect(data), n_rows)
        } else {
          shiny::req(rv$selected_schema, rv$selected_table)
          data <- db_lake_read(schema = rv$selected_schema, table = rv$selected_table)
          head(dplyr::collect(data), n_rows)
        }
      }, error = function(e) {
        data.frame(Error = e$message)
      })
    })
    
    output$preview_table <- DT::renderDataTable({
      shiny::req(preview_data())
      DT::datatable(
        preview_data(),
        options = list(
          pageLength = 25,
          scrollX = TRUE,
          dom = 'Bfrtip'
        ),
        filter = "top",
        rownames = FALSE
      )
    })
    
    # =========================================================================
    # Metadata Tab
    # =========================================================================
    
    output$metadata_display <- shiny::renderUI({
      if (is_hive) {
        shiny::req(rv$selected_section, rv$selected_dataset)
        meta <- tryCatch(
          db_get_docs(section = rv$selected_section, dataset = rv$selected_dataset),
          error = function(e) list()
        )
      } else {
        shiny::req(rv$selected_schema, rv$selected_table)
        meta <- tryCatch(
          db_get_docs(schema = rv$selected_schema, table = rv$selected_table),
          error = function(e) list()
        )
      }
      
      .render_metadata_card(meta, is_hive, rv, ns)
    })
    
    # =========================================================================
    # Search Tab
    # =========================================================================
    
    search_results <- shiny::eventReactive(input$do_search, {
      pattern <- input$search_pattern
      field <- input$search_field
      
      if (is.null(pattern) || nchar(pattern) == 0) {
        return(data.frame(Message = "Enter a search pattern"))
      }
      
      tryCatch({
        db_search(pattern, field = field)
      }, error = function(e) {
        data.frame(Error = e$message)
      })
    })
    
    output$search_results <- DT::renderDataTable({
      DT::datatable(
        search_results(),
        options = list(pageLength = 10, scrollX = TRUE),
        rownames = FALSE,
        selection = "single"
      )
    })
    
    col_search_results <- shiny::eventReactive(input$do_col_search, {
      pattern <- input$col_search_pattern
      
      if (is.null(pattern) || nchar(pattern) == 0) {
        return(data.frame(Message = "Enter a column name pattern"))
      }
      
      tryCatch({
        db_search_columns(pattern)
      }, error = function(e) {
        data.frame(Error = e$message)
      })
    })
    
    output$col_search_results <- DT::renderDataTable({
      DT::datatable(
        col_search_results(),
        options = list(pageLength = 10, scrollX = TRUE),
        rownames = FALSE
      )
    })
    
    # =========================================================================
    # Dictionary Tab
    # =========================================================================
    
    dictionary_data <- shiny::eventReactive(input$gen_dict, {
      include_cols <- input$dict_include_cols
      
      tryCatch({
        db_dictionary(include_columns = include_cols)
      }, error = function(e) {
        data.frame(Error = e$message)
      })
    })
    
    output$dictionary_table <- DT::renderDataTable({
      shiny::req(dictionary_data())
      DT::datatable(
        dictionary_data(),
        options = list(
          pageLength = 25,
          scrollX = TRUE,
          dom = 'Bfrtip'
        ),
        filter = "top",
        rownames = FALSE
      )
    })
    
    output$download_dict <- shiny::downloadHandler(
      filename = function() {
        paste0("data_dictionary_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv")
      },
      content = function(file) {
        utils::write.csv(dictionary_data(), file, row.names = FALSE)
      }
    )
    
    # =========================================================================
    # Connection Info Tab
    # =========================================================================

    output$connection_info <- shiny::renderPrint({
      db_status()
    })

    # =========================================================================
    # Public Catalog Tab (Both Modes)
    # =========================================================================

    # Reactive to track catalog refresh
    public_catalog_refresh <- shiny::reactiveVal(0)

    # Refresh button
    shiny::observeEvent(input$refresh_public, {
      public_catalog_refresh(public_catalog_refresh() + 1)
    })

    if (is_hive) {
      # ---- Hive Mode ----

      # Public catalog data
      public_catalog_data <- shiny::reactive({
        public_catalog_refresh()  # Trigger refresh
        tryCatch({
          db_list_public()
        }, error = function(e) {
          data.frame(
            section = character(),
            dataset = character(),
            description = character(),
            owner = character(),
            tags = character()
          )
        })
      })

      output$public_catalog_table <- DT::renderDataTable({
        DT::datatable(
          public_catalog_data(),
          options = list(pageLength = 15, scrollX = TRUE),
          rownames = FALSE,
          selection = "single"
        )
      })

      # Sync catalog button
      shiny::observeEvent(input$sync_catalog, {
        tryCatch({
          result <- db_sync_catalog()
          shiny::showNotification(
            paste0("Sync complete: ", result$synced, " synced"),
            type = "message"
          )
          public_catalog_refresh(public_catalog_refresh() + 1)
        }, error = function(e) {
          shiny::showNotification(
            paste0("Sync error: ", e$message),
            type = "error"
          )
        })
      })

      # Display public status of selected dataset
      output$public_status_display <- shiny::renderUI({
        if (is.null(rv$selected_section) || is.null(rv$selected_dataset)) {
          return(shiny::tags$em("No dataset selected"))
        }

        is_public <- tryCatch(
          db_is_public(section = rv$selected_section, dataset = rv$selected_dataset),
          error = function(e) FALSE
        )

        shiny::tags$div(
          shiny::tags$strong(paste0(rv$selected_section, "/", rv$selected_dataset, ": ")),
          if (is_public) {
            shiny::tags$span(
              class = "badge bg-success",
              shiny::icon("globe"), " Public"
            )
          } else {
            shiny::tags$span(
              class = "badge bg-secondary",
              shiny::icon("lock"), " Private"
            )
          }
        )
      })

      # Make public button
      shiny::observeEvent(input$make_public, {
        shiny::req(rv$selected_section, rv$selected_dataset)

        tryCatch({
          db_set_public(section = rv$selected_section, dataset = rv$selected_dataset)
          shiny::showNotification(
            paste0("Published ", rv$selected_section, "/", rv$selected_dataset, " to catalog"),
            type = "message"
          )
          public_catalog_refresh(public_catalog_refresh() + 1)
        }, error = function(e) {
          shiny::showNotification(
            paste0("Error: ", e$message),
            type = "error"
          )
        })
      })

      # Make private button
      shiny::observeEvent(input$make_private, {
        shiny::req(rv$selected_section, rv$selected_dataset)

        tryCatch({
          db_set_private(section = rv$selected_section, dataset = rv$selected_dataset)
          shiny::showNotification(
            paste0("Removed ", rv$selected_section, "/", rv$selected_dataset, " from catalog"),
            type = "message"
          )
          public_catalog_refresh(public_catalog_refresh() + 1)
        }, error = function(e) {
          shiny::showNotification(
            paste0("Error: ", e$message),
            type = "error"
          )
        })
      })

    } else {
      # ---- DuckLake Mode ----

      # Current section display
      output$current_section_display <- shiny::renderUI({
        section <- tryCatch(db_current_section(), error = function(e) NULL)
        if (is.null(section)) {
          shiny::tags$span(
            class = "badge bg-warning",
            shiny::icon("exclamation-triangle"), " No section set"
          )
        } else {
          shiny::tags$span(
            class = "badge bg-info",
            shiny::icon("database"), " Section: ", section
          )
        }
      })

      # Public catalog data from master catalog
      public_catalog_data <- shiny::reactive({
        public_catalog_refresh()  # Trigger refresh
        tryCatch({
          db_list_public()
        }, error = function(e) {
          data.frame(
            section = character(),
            schema = character(),
            table = character(),
            description = character(),
            owner = character()
          )
        })
      })

      output$public_catalog_table <- DT::renderDataTable({
        DT::datatable(
          public_catalog_data(),
          options = list(pageLength = 10, scrollX = TRUE),
          rownames = FALSE,
          selection = "single"
        )
      })

      # Registered sections
      registered_sections_data <- shiny::reactive({
        public_catalog_refresh()  # Trigger refresh
        tryCatch({
          db_list_registered_sections()
        }, error = function(e) {
          data.frame(
            section_name = character(),
            description = character(),
            owner = character()
          )
        })
      })

      output$registered_sections_table <- DT::renderDataTable({
        DT::datatable(
          registered_sections_data(),
          options = list(pageLength = 5, scrollX = TRUE),
          rownames = FALSE,
          selection = "single"
        )
      })

      # Display public status of selected table
      output$public_status_display <- shiny::renderUI({
        if (is.null(rv$selected_schema) || is.null(rv$selected_table)) {
          return(shiny::tags$em("No table selected"))
        }

        is_public <- tryCatch(
          db_is_public(schema = rv$selected_schema, table = rv$selected_table),
          error = function(e) FALSE
        )

        shiny::tags$div(
          shiny::tags$strong(paste0(rv$selected_schema, ".", rv$selected_table, ": ")),
          if (is_public) {
            shiny::tags$span(
              class = "badge bg-success",
              shiny::icon("globe"), " Public"
            )
          } else {
            shiny::tags$span(
              class = "badge bg-secondary",
              shiny::icon("lock"), " Private"
            )
          }
        )
      })

      # Make public button (DuckLake)
      shiny::observeEvent(input$make_public, {
        shiny::req(rv$selected_schema, rv$selected_table)

        tryCatch({
          db_set_public(schema = rv$selected_schema, table = rv$selected_table)
          shiny::showNotification(
            paste0("Published ", rv$selected_schema, ".", rv$selected_table, " to master catalog"),
            type = "message"
          )
          public_catalog_refresh(public_catalog_refresh() + 1)
        }, error = function(e) {
          shiny::showNotification(
            paste0("Error: ", e$message),
            type = "error"
          )
        })
      })

      # Make private button (DuckLake)
      shiny::observeEvent(input$make_private, {
        shiny::req(rv$selected_schema, rv$selected_table)

        tryCatch({
          db_set_private(schema = rv$selected_schema, table = rv$selected_table)
          shiny::showNotification(
            paste0("Removed ", rv$selected_schema, ".", rv$selected_table, " from master catalog"),
            type = "message"
          )
          public_catalog_refresh(public_catalog_refresh() + 1)
        }, error = function(e) {
          shiny::showNotification(
            paste0("Error: ", e$message),
            type = "error"
          )
        })
      })
    }

  })
}


# =============================================================================
# Helper Functions
# =============================================================================

#' Render hive mode tree view
#' @noRd
.render_hive_tree <- function(ns, rv) {
  sections <- tryCatch(db_list_sections(), error = function(e) character(0))
  
  if (length(sections) == 0) {
    return(shiny::tags$em("No sections found"))
  }
  
  # Build tree
  tree_items <- lapply(sections, function(sec) {
    datasets <- tryCatch(db_list_datasets(sec), error = function(e) character(0))
    
    if (length(datasets) == 0) {
      return(
        shiny::tags$div(
          class = "tree-section",
          shiny::tags$span(
            shiny::icon("folder"),
            " ", sec,
            style = "font-weight: bold; cursor: pointer;"
          ),
          shiny::tags$div(
            class = "tree-datasets",
            style = "margin-left: 20px;",
            shiny::tags$em("(empty)")
          )
        )
      )
    }
    
    dataset_items <- lapply(datasets, function(ds) {
      item_id <- paste0(sec, "/", ds)
      shiny::tags$div(
        class = "tree-dataset",
        style = "margin-left: 20px; cursor: pointer; padding: 2px 5px;",
        onclick = sprintf("Shiny.setInputValue('%s', '%s', {priority: 'event'})", 
                          ns("tree_selection"), item_id),
        shiny::icon("table"),
        " ", ds
      )
    })
    
    shiny::tags$div(
      class = "tree-section",
      style = "margin-bottom: 10px;",
      shiny::tags$details(
        open = TRUE,
        shiny::tags$summary(
          style = "font-weight: bold; cursor: pointer;",
          shiny::icon("folder-open"),
          " ", sec
        ),
        shiny::tags$div(dataset_items)
      )
    )
  })
  
  shiny::tags$div(
    class = "tree-view",
    style = "max-height: 500px; overflow-y: auto;",
    tree_items,
    # CSS for hover effect
    shiny::tags$style("
      .tree-dataset:hover { background-color: #e9ecef; border-radius: 3px; }
    ")
  )
}


#' Render DuckLake mode tree view
#' @noRd
.render_ducklake_tree <- function(ns, rv) {
  schemas <- tryCatch(db_list_schemas(), error = function(e) character(0))
  
  # Filter out metadata schema
  schemas <- schemas[!grepl("^_", schemas)]
  
  if (length(schemas) == 0) {
    return(shiny::tags$em("No schemas found"))
  }
  
  tree_items <- lapply(schemas, function(sch) {
    tables <- tryCatch(db_list_tables(sch), error = function(e) character(0))
    views <- tryCatch(db_list_views(sch), error = function(e) character(0))
    
    if (length(tables) == 0 && length(views) == 0) {
      return(
        shiny::tags$div(
          class = "tree-schema",
          shiny::tags$span(
            shiny::icon("database"),
            " ", sch,
            style = "font-weight: bold;"
          ),
          shiny::tags$div(
            style = "margin-left: 20px;",
            shiny::tags$em("(empty)")
          )
        )
      )
    }
    
    table_items <- lapply(tables, function(tbl) {
      item_id <- paste0(sch, ".", tbl)
      shiny::tags$div(
        class = "tree-table",
        style = "margin-left: 20px; cursor: pointer; padding: 2px 5px;",
        onclick = sprintf("Shiny.setInputValue('%s', '%s', {priority: 'event'})", 
                          ns("tree_selection_lake"), item_id),
        shiny::icon("table"),
        " ", tbl
      )
    })
    
    view_items <- lapply(views, function(v) {
      shiny::tags$div(
        class = "tree-view-item",
        style = "margin-left: 20px; padding: 2px 5px; color: #6c757d;",
        shiny::icon("eye"),
        " ", v, " (view)"
      )
    })
    
    shiny::tags$div(
      class = "tree-schema",
      style = "margin-bottom: 10px;",
      shiny::tags$details(
        open = TRUE,
        shiny::tags$summary(
          style = "font-weight: bold; cursor: pointer;",
          shiny::icon("database"),
          " ", sch
        ),
        shiny::tags$div(table_items),
        shiny::tags$div(view_items)
      )
    )
  })
  
  shiny::tags$div(
    class = "tree-view",
    style = "max-height: 500px; overflow-y: auto;",
    tree_items,
    shiny::tags$style("
      .tree-table:hover { background-color: #e9ecef; border-radius: 3px; }
    ")
  )
}


#' Render metadata card
#' @noRd
.render_metadata_card <- function(meta, is_hive, rv, ns) {

  # Title
  if (is_hive) {
    title <- paste0(rv$selected_section, "/", rv$selected_dataset)

    # Check public status
    is_public <- tryCatch(
      db_is_public(section = rv$selected_section, dataset = rv$selected_dataset),
      error = function(e) FALSE
    )
  } else {
    title <- paste0(rv$selected_schema, ".", rv$selected_table)

    # Check public status in DuckLake mode
    is_public <- tryCatch(
      db_is_public(schema = rv$selected_schema, table = rv$selected_table),
      error = function(e) FALSE
    )
  }

  # Basic info
  desc <- meta$description %||% "(No description)"
  owner <- meta$owner %||% "(No owner)"
  tags <- if (length(meta$tags) > 0) paste(meta$tags, collapse = ", ") else "(No tags)"
  
  # Column documentation
  col_docs <- if (length(meta$columns) > 0) {
    col_rows <- lapply(names(meta$columns), function(col_name) {
      col <- meta$columns[[col_name]]
      shiny::tags$tr(
        shiny::tags$td(shiny::tags$code(col_name)),
        shiny::tags$td(col$description %||% ""),
        shiny::tags$td(col$units %||% "")
      )
    })
    
    shiny::tags$div(
      shiny::tags$h5("Column Documentation"),
      shiny::tags$table(
        class = "table table-sm table-striped",
        shiny::tags$thead(
          shiny::tags$tr(
            shiny::tags$th("Column"),
            shiny::tags$th("Description"),
            shiny::tags$th("Units")
          )
        ),
        shiny::tags$tbody(col_rows)
      )
    )
  } else {
    shiny::tags$p(shiny::tags$em("No column documentation"))
  }
  
  # Public badge
  public_badge <- if (is_public) {
    shiny::tags$span(
      class = "badge bg-success ms-2",
      shiny::icon("globe"), " Public"
    )
  } else {
    NULL
  }

  # Build card
  shiny::tags$div(
    class = "card",
    shiny::tags$div(
      class = "card-header",
      shiny::tags$h4(title, public_badge)
    ),
    shiny::tags$div(
      class = "card-body",
      shiny::tags$dl(
        class = "row",
        shiny::tags$dt(class = "col-sm-3", "Description"),
        shiny::tags$dd(class = "col-sm-9", desc),
        shiny::tags$dt(class = "col-sm-3", "Owner"),
        shiny::tags$dd(class = "col-sm-9", owner),
        shiny::tags$dt(class = "col-sm-3", "Tags"),
        shiny::tags$dd(class = "col-sm-9", tags)
      ),
      shiny::hr(),
      col_docs
    )
  )
}
