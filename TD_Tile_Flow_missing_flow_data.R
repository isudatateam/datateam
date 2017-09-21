# LIBRARIES ----------------------------------------------------------------------
library(tidyverse)
library(lubridate)
library(readxl)
library(stringr)
library(scales)
library(zoo)


# PART >>> 1 ---------------------------------

# SERF_SD =====================================
# SERF_SD tile flow 2015-2016
SERF_SD_2015 <- read_excel("~/TD/Research Data/SERF_SD/Water_Data_20170821.xlsx", 
                           sheet = "Flow_2015", 
                           col_types = c("date", "text", "text", 
                                         "skip", "skip", "skip", "skip"))
SERF_SD_2016 <- read_excel("~/TD/Research Data/SERF_SD/Water_Data_20170821.xlsx", 
                           sheet = "Flow_2016", 
                           col_types = c("date", "text", "text", 
                                         "skip", "skip", "skip", "skip", "skip"), 
                           skip = 1)


serf_sd <- SERF_SD_2016 %>%
  mutate(Date = floor_date(Date, unit = "minute")) %>%
  bind_rows(SERF_SD_2015) %>%
  gather(key = plotid, value = comment, 2:3) %>%
  mutate(flow = as.double(comment),
         comment = ifelse(!is.na(flow), "Collected", comment)) %>%
  arrange(plotid, Date)


serf_sd %>%
  group_by(plotid) %>%
  mutate(gap_length = rep(rle(comment)[[1]], rle(comment)[[1]]),
         gap_number = rep(1:length(rle(comment)[[1]]), rle(comment)[[1]])) %>%
  group_by(plotid, gap_number) %>%
  filter(row_number() == 1 | row_number() == n()) %>%
  mutate(first_last = ifelse(row_number()== 1, "start_date", "end_date")) %>%
  group_by(plotid) %>% 
  mutate(flow = ifelse(first_last == "end_date" & Date < max(Date),
                       "D", "OK")) %>%
  filter(flow == "OK") %>%
  mutate(start_time = Date,
         end_time = lead(Date)) %>% 
  select(-c(flow, first_last, Date)) %>%
  filter(!is.na(end_time)) %>%
  mutate(time_gap = difftime(end_time, start_time, units = "secs")) %>%
  mutate(site = "SERF_SD") %>%
  select(site, everything()) %>%
  rename(data_status = comment) %>% 
  ungroup() -> serf_sd_missing_data

serf_sd_missing_data %>%
  select(-c(gap_number, gap_length)) %>%
  filter(data_status != "Collected") %>%
  mutate(time_gap = as.period(time_gap)) %>%
  write.csv(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/SERF_SD_missing_data.csv", row.names = FALSE)


serf_sd %>%
  mutate(comment = ifelse(comment == "Did not collect", "Missing", comment)) %>%
  ggplot(aes(x = Date, y = plotid, colour = comment)) +
  geom_point(shape = "|", size = 15) +
  scale_color_manual(values = c("skyblue3", "firebrick1", "black"),
                     labels = function(x) str_wrap(x, width = 12)) +
  scale_x_datetime(labels = date_format("%m-%Y")) + 
  ggtitle("SERF_SD", subtitle = "Tile Flow Data") +
  theme_bw() + 
  guides(colour = guide_legend(override.aes = list(size = 7))) +
  theme(#panel.grid = element_blank(),
    plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(size = 14, hjust = 0.5),
    axis.title = element_blank(),
    axis.text = element_text(size = 12),
    legend.title = element_blank(),
    legend.text = element_text(size = 10),
    legend.key.size = unit(1, "cm"))
ggsave(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/SERF_SD_missing_data.png", width = 8, height = 4)



# DPAC ===========================
dpac <- data.frame(year = 0)
for (i in 2012:2016) {
  read_excel("~/TD/Research Data/DPAC/DPAC Tile Flow_20170822.xlsx", sheet = as.character(i),
             col_names = FALSE, skip = 2, col_types = c(rep("date", 2), rep("numeric", 4))) -> temp
  dpac <- bind_rows(dpac, temp)
}

dpac %>%
  rename(Date = X__1, Time = X__2, NW = X__3, SW = X__4, NE = X__5, SE = X__6) %>%
  filter(!is.na(Date)) %>%
  mutate(Date = ymd_hm(paste(Date, format(Time, "%H:%M")))) %>%
  select(-c(1,3)) %>%
  gather(key = plotid, value = flow, NW:SE) %>%
  mutate(comment = ifelse(is.na(flow), "Did not collect", "Collected")) %>%
  ungroup() %>%
  as.tibble() -> dpac

dpac %>%
  group_by(plotid) %>%
  mutate(gap_length = rep(rle(comment)[[1]], rle(comment)[[1]]),
         gap_number = rep(1:length(rle(comment)[[1]]), rle(comment)[[1]])) %>%
  group_by(plotid, gap_number) %>%
  filter(row_number() == 1 | row_number() == n()) %>%
  mutate(first_last = ifelse(row_number()== 1, "start_date", "end_date")) %>%
  group_by(plotid) %>% 
  mutate(flow = ifelse(first_last == "end_date" & Date < max(Date),
                       "D", "OK")) %>%
  filter(flow == "OK") %>%
  mutate(start_time = Date,
         end_time = lead(Date)) %>% 
  select(-c(flow, first_last, Date)) %>%
  filter(!is.na(end_time)) %>%
  mutate(time_gap = difftime(end_time, start_time, units = "secs")) %>%
  mutate(site = "DPAC") %>%
  select(site, everything()) %>%
  rename(data_status = comment) %>% 
  ungroup() -> dpac_missing_data

dpac_missing_data %>%
  select(-c(gap_number, gap_length)) %>%
  filter(data_status != "Collected") %>%
  mutate(time_gap = as.period(time_gap)) %>%
  write.csv(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/DPAC_missing_data.csv", row.names = FALSE)


dpac %>%
  mutate(comment = ifelse(comment == "Did not collect", "Missing", comment)) %>%
  # filter(year(Date) == 2016) %>%
  ggplot(aes(x = Date, y = plotid, colour = comment)) +
  geom_point(shape = "|", size = 15) +
  scale_color_manual(values = c("skyblue3", "firebrick1", "black")) +
  scale_x_datetime() + 
  ggtitle("DPAC", subtitle = "Tile Flow Data") +
  theme_bw() + 
  guides(colour = guide_legend(override.aes = list(size = 7))) +
  theme(#panel.grid = element_blank(),
    plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(size = 14, hjust = 0.5),
    axis.title = element_blank(),
    axis.text = element_text(size = 12),
    legend.title = element_blank(),
    legend.text = element_text(size = 10),
    legend.key.size = unit(1, "cm")) 
ggsave(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/DPAC_missing_data.png", width = 8, height = 4)



# SERF_IA ===================
serf_ia <- read_csv("~/TD/Research Data/SERF_IA/SERF_Drainage_Data_Flow_HOURLY.csv", 
                    col_types = cols(`S1 WAT1` = col_double(),`S2 WAT1` = col_double(), 
                                     `S3 WAT1` = col_double(), `S4 WAT1` = col_double(), 
                                     `S5 WAT1` = col_double(), `S6 WAT1` = col_double(), 
                                     date = col_datetime(format = "%Y-%m-%d %H:%M:%S"))) %>%
  gather(key = plotid, value = flow, 2:7) %>%
  separate(plotid, into = c("plotid", "code"), sep = " ") %>%
  mutate(code = ifelse(is.na(flow), "Did not collect", "Collected")) %>%
  rename(Date = date, comment = code)


serf_ia %>%
  group_by(plotid) %>%
  mutate(gap_length = rep(rle(comment)[[1]], rle(comment)[[1]]),
         gap_number = rep(1:length(rle(comment)[[1]]), rle(comment)[[1]])) %>%
  group_by(plotid, gap_number) %>%
  filter(row_number() == 1 | row_number() == n()) %>%
  mutate(first_last = ifelse(row_number()== 1, "start_date", "end_date")) %>%
  group_by(plotid) %>% 
  mutate(flow = ifelse(first_last == "end_date" & Date < max(Date),
                       "D", "OK")) %>%
  filter(flow == "OK") %>%
  mutate(start_time = Date,
         end_time = lead(Date)) %>% 
  select(-c(flow, first_last, Date)) %>%
  filter(!is.na(end_time)) %>%
  mutate(time_gap = difftime(end_time, start_time, units = "secs")) %>%
  mutate(site = "SERF_IA") %>%
  select(site, everything()) %>%
  rename(data_status = comment) %>% 
  ungroup() -> serf_ia_missing_data

serf_ia_missing_data %>%
  select(-c(gap_number, gap_length)) %>%
  filter(data_status != "Collected") %>%
  mutate(time_gap = as.period(time_gap)) %>%
  write.csv(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/SERF_IA_missing_data.csv", row.names = FALSE)


serf_ia %>%
  mutate(comment = ifelse(is.na(flow), "Missing", "Collected")) %>%
  ggplot(aes(x = Date, y = plotid, colour = comment)) +
  geom_point(shape = "|", size = 10) +
  scale_color_manual(values = c("skyblue3", "firebrick1", "black")) +
  scale_x_datetime() + 
  ggtitle("SERF_IA", subtitle = "Tile Flow Data") +
  theme_bw() + 
  guides(colour = guide_legend(override.aes = list(size = 7))) +
  theme(#panel.grid = element_blank(),
    plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(size = 14, hjust = 0.5),
    axis.title = element_blank(),
    axis.text = element_text(size = 12),
    legend.title = element_blank(),
    legend.text = element_text(size = 10),
    legend.key.size = unit(1, "cm")) 
ggsave(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/SERF_IA_missing_data.png", width = 8, height = 4)


# SOTRY ========================
story <- data.frame(year = NA)
for (i in 2006:2009) {
  read_excel("~/TD/Research Data/STORY/STORY Tile Flow.xlsx", sheet = as.character(i),
             col_types = c(rep("date", 2), rep("numeric", 12))) %>%
    filter(row_number() > 1) %>%
    mutate(year = i) -> temp
  story <- bind_rows(story, temp)
}
story %>%
  gather(key = plotid, value = flow, 4:15) %>%
  separate(col = plotid, into = c("plotid", "comment"), sep = " WAT1 ") %>%
  filter(plotid %in% c(2,3,5,8,9,11)) %>%
  select(Date, plotid, comment, flow) %>%
  mutate(comment = ifelse(is.na(flow), "Did not collect", "Collected")) %>%
  filter(!is.na(Date)) %>%
  as.tibble() -> story

story %>%
  group_by(plotid) %>%
  mutate(gap_length = rep(rle(comment)[[1]], rle(comment)[[1]]),
         gap_number = rep(1:length(rle(comment)[[1]]), rle(comment)[[1]])) %>%
  group_by(plotid, gap_number) %>%
  filter(row_number() == 1 | row_number() == n()) %>%
  mutate(first_last = ifelse(row_number()== 1, "start_date", "end_date")) %>%
  group_by(plotid) %>% 
  mutate(flow = ifelse(first_last == "end_date" & Date < max(Date),
                       "D", "OK")) %>%
  filter(flow == "OK") %>%
  mutate(start_time = Date,
         end_time = lead(Date)) %>% 
  select(-c(flow, first_last, Date)) %>%
  filter(!is.na(end_time)) %>%
  mutate(time_gap = difftime(end_time, start_time, units = "secs")) %>%
  mutate(site = "STORY") %>%
  select(site, everything()) %>%
  rename(data_status = comment) %>% 
  ungroup() -> story_missing_data

story_missing_data %>%
  select(-c(gap_number, gap_length)) %>%
  mutate(time_gap = as.period(time_gap)) %>%
  filter(data_status != "Collected") %>%
  write.csv(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/STORY_missing_data.csv", row.names = FALSE)


story %>%
  mutate(comment = ifelse(is.na(flow), "Missing", "Collected")) %>%
  ggplot(aes(x = Date, y = plotid, colour = comment)) +
  geom_point(shape = "|", size = 10) +
  scale_color_manual(values = c("skyblue3", "firebrick1", "black")) +
  scale_x_datetime() + 
  ggtitle("STORY", subtitle = "Tile Flow Data (daily)") +
  theme_bw() + 
  guides(colour = guide_legend(override.aes = list(size = 7))) +
  theme(#panel.grid = element_blank(),
    plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(size = 14, hjust = 0.5),
    axis.title = element_blank(),
    axis.text = element_text(size = 12),
    legend.title = element_blank(),
    legend.text = element_text(size = 10),
    legend.key.size = unit(1, "cm")) 
ggsave(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/STORY_missing_data.png", width = 8, height = 4)






# HICKS_B ===================
hicks_b <- read_csv("C:/Users/Gio/Documents/TD/Research Data/HICKS.B/HICKS_B WAT1_2011-2015.csv", 
                    col_types = cols(`BE` = col_double(),`BW` = col_double(),
                                     Date = col_datetime(format = "%Y-%m-%d %H:%M:%S"))) %>%
  gather(key = plotid, value = flow, BE:BW) %>%
  mutate(comment = ifelse(is.na(flow), "Did not collect", "Collected"))

hicks <- data.frame(Time = NA)
for (i in 2006:2010) {
  read_excel("C:/Users/Gio/Documents/TD/Research Data/HICKS.B/HICKS_B Tile Flow.xlsx", 
             sheet = as.character(i),
             col_types = c(rep("date", 2), rep("numeric", 2))) %>%
    filter(row_number() > 1) -> temp
  hicks <- bind_rows(hicks, temp)
}

# hicks_b %>%
#   mutate(Date2 = Date + hours(1)) %>%
#   mutate(Date3 = ifelse(between(Date, ymd_hm("20120311 0200"), ymd_hm("20121104 0100")), 
#                        Date2, Date),
#          Date3 = ifelse(between(Date, ymd_hm("20130310 0200"), ymd_hm("20131103 0100")), 
#                         Date2, Date3),
#          Date3 = ifelse(between(Date, ymd_hm("20140309 0200"), ymd_hm("20141102 0100")), 
#                         Date2, Date3),
#          Date3 = ifelse(between(Date, ymd_hm("20150308 0200"), ymd_hm("20151101 0100")), 
#                         Date2, Date3),
#          Date3 = as_datetime(Date3)) %>%
#   mutate(Date = Date3) %>%
#   select(Date, plotid, flow, comment) -> hicks_b



hicks %>%
  gather(key = plotid, value = flow, 3:4) %>%
  mutate(plotid = word(plotid, 1),
         comment = ifelse(is.na(flow), "Did not collect", "Collected")) %>%
  select(-Time) %>% 
  as.tibble() %>%
  filter(!is.na(Date)) %>%
  bind_rows(hicks_b) %>% 
  arrange(plotid, Date) %>%
  ungroup() -> hicks_b


hicks_b %>%
  group_by(plotid) %>%
  mutate(gap_length = rep(rle(comment)[[1]], rle(comment)[[1]]),
         gap_number = rep(1:length(rle(comment)[[1]]), rle(comment)[[1]])) %>%
  group_by(plotid, gap_number) %>%
  filter(row_number() == 1 | row_number() == n()) %>%
  mutate(first_last = ifelse(row_number()== 1, "start_date", "end_date")) %>%
  group_by(plotid) %>% 
  mutate(flow = ifelse(first_last == "end_date" & Date < max(Date),
                       "D", "OK")) %>%
  filter(flow == "OK") %>%
  mutate(start_time = Date,
         end_time = lead(Date)) %>% 
  select(-c(flow, first_last, Date)) %>%
  filter(!is.na(end_time)) %>%
  mutate(time_gap = difftime(end_time, start_time, units = "secs")) %>%
  mutate(site = "HICK_B") %>%
  select(site, everything()) %>%
  rename(data_status = comment) %>% 
  ungroup() -> hicks_b_missing_data

hicks_b_missing_data %>%
  select(-c(gap_number, gap_length)) %>%
  mutate(time_gap = as.period(time_gap)) %>%
  filter(data_status != "Collected") %>%
  write.csv(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/HICKS_B_missing_data.csv", row.names = FALSE)


hicks_b %>%
  mutate(comment = ifelse(is.na(flow), "Missing", "Collected")) %>%
  ggplot(aes(x = Date, y = plotid, colour = comment)) +
  geom_point(shape = "|", size = 15) +
  scale_color_manual(values = c("skyblue3", "firebrick1", "black")) +
  scale_x_datetime() + 
  ggtitle("HICKS_B", subtitle = "Tile Flow Data (daily 2006-2010, hourly 2011-2015)") +
  theme_bw() + 
  guides(colour = guide_legend(override.aes = list(size = 7))) +
  theme(#panel.grid = element_blank(),
    plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(size = 14, hjust = 0.5),
    axis.title = element_blank(),
    axis.text = element_text(size = 12),
    legend.title = element_blank(),
    legend.text = element_text(size = 10),
    legend.key.size = unit(1, "cm")) 
ggsave(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/HICKS_B_missing_data.png", width = 8, height = 4)






# STJOHNS ===========================
stj <- read_excel("~/TD/Research Data/STJOHNS/STJOHNS Tile Flow_20170822.xlsx", 
                  sheet = "2008", 
                  col_types = c("date", "date", "numeric", "numeric"))
for (i in 2009:2015) {
  read_excel("~/TD/Research Data/STJOHNS/STJOHNS Tile Flow_20170822.xlsx", 
             sheet = as.character(i),
             col_types = c(rep("date", 2), rep("numeric", 2))) %>%
    filter(row_number() > 1) -> temp
  stj <- bind_rows(stj, temp)
}

stj %>%
  gather(key = plotid, value = flow, 3:4) %>%
  mutate(plotid = word(plotid, 1),
         comment = ifelse(is.na(flow), "Did not collect", "Collected")) %>%
  filter(!is.na(Date)) %>%
  mutate(Date = ymd_hm(paste(Date, format(Time, "%H:%M")))) %>%
  select(-Time) %>%
  arrange(plotid, Date) %>%
  ungroup() -> stj_combined

stj_combined %>%
  mutate(Year = year(Date)) %>%
  group_by(plotid, Year) %>%
  mutate(Date2 = lead(Date), 
         Freq = difftime(Date2, Date, units = "mins")) %>%
  mutate(Freq = na.locf(Freq,  maxgap = 1, na.rm = FALSE, fromLast = F)) %>%
  mutate(mins = minute(Date),
         Date2 = ifelse(mins %in% c(15, 45), Date - minutes(15), Date),
         Date2 = as_datetime(Date2),
         na_ditect = is.na(flow)) %>%
  group_by(Date2, plotid) %>%
  summarise(flow = sum(flow, na.rm = TRUE),
            na_ditect = sum(na_ditect),
            check_time = mean(Freq),
            Year = mean(Year)) %>%
  mutate(flow = ifelse(na_ditect == 2, NA, flow)) %>%
  mutate(flow = ifelse(na_ditect == 1 & check_time == 30, NA, flow)) %>%
  rename(Date = Date2) %>%
  ungroup() %>%
  select(Date:flow) %>%
  mutate(comment = ifelse(is.na(flow), "Did not collect", "Collected")) %>%
  arrange(plotid, Date) -> stjohns



stjohns %>%
  group_by(plotid) %>%
  mutate(gap_length = rep(rle(comment)[[1]], rle(comment)[[1]]),
         gap_number = rep(1:length(rle(comment)[[1]]), rle(comment)[[1]])) %>%
  group_by(plotid, gap_number) %>%
  filter(row_number() == 1 | row_number() == n()) %>%
  mutate(first_last = ifelse(row_number()== 1, "start_date", "end_date")) %>%
  group_by(plotid) %>% 
  mutate(flow = ifelse(first_last == "end_date" & Date < max(Date),
                       "D", "OK")) %>%
  filter(flow == "OK") %>%
  mutate(start_time = Date,
         end_time = lead(Date)) %>% 
  select(-c(flow, first_last, Date)) %>%
  filter(!is.na(end_time)) %>%
  mutate(time_gap = difftime(end_time, start_time, units = "secs")) %>%
  mutate(site = "STJOHNS") %>%
  select(site, everything()) %>%
  rename(data_status = comment) %>% 
  ungroup() -> stjohns_missing_data

stjohns_missing_data %>%
  select(-c(gap_number, gap_length)) %>%
  filter(data_status != "Collected") %>%
  mutate(time_gap = as.period(time_gap)) %>%
  write.csv(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/STJOHNS_missing_data.csv", row.names = FALSE)


stjohns %>%
  mutate(comment = ifelse(is.na(flow), "Missing", "Collected")) %>%
  ggplot(aes(x = Date, y = plotid, colour = comment)) +
  geom_point(shape = "|", size = 15) +
  scale_color_manual(values = c("skyblue3", "firebrick1", "black")) +
  scale_x_datetime() + 
  ggtitle("STJOHNS", subtitle = "Tile Flow Data") +
  theme_bw() + 
  guides(colour = guide_legend(override.aes = list(size = 7))) +
  theme(#panel.grid = element_blank(),
    plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(size = 14, hjust = 0.5),
    axis.title = element_blank(),
    axis.text = element_text(size = 12),
    legend.title = element_blank(),
    legend.text = element_text(size = 10),
    legend.key.size = unit(1, "cm")) 
ggsave(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/STJOHNS_missing_data.png", width = 8, height = 4)







# MUDS2 ===========================
muds <-  tibble(Time = 0)
read_excel(path = "~/TD/Research Data/MUDS2/MUDS2 Tile Flow_20170823.xlsx",
           sheet = "2010", n_max = 1) %>% names() -> muds_names
for (i in 2010:2013) {
  read_excel("~/TD/Research Data/MUDS2/MUDS2 Tile Flow_20170823.xlsx", 
             sheet = as.character(i),
             skip = 2, 
             col_types = c(rep("date", 2), rep("numeric", 8)),
             col_names = muds_names) -> temp
  muds <- bind_rows(muds, temp)
}


muds %>%
  filter(!is.na(Date)) %>%
  select(-Time) %>%
  gather(key = plotid, value = flow, 2:9) %>%
  mutate(plotid = word(plotid, 1)) %>%
  mutate(comment = ifelse(is.na(flow), "Did not collect", "Collected")) %>%
  ungroup() -> muds2

muds2 %>%
  group_by(plotid) %>%
  mutate(gap_length = rep(rle(comment)[[1]], rle(comment)[[1]]),
         gap_number = rep(1:length(rle(comment)[[1]]), rle(comment)[[1]])) %>%
  group_by(plotid, gap_number) %>%
  filter(row_number() == 1 | row_number() == n()) %>%
  mutate(first_last = ifelse(row_number()== 1, "start_date", "end_date")) %>%
  group_by(plotid) %>% 
  mutate(flow = ifelse(first_last == "end_date" & Date < max(Date),
                       "D", "OK")) %>%
  filter(flow == "OK") %>%
  mutate(start_time = Date,
         end_time = lead(Date)) %>% 
  select(-c(flow, first_last, Date)) %>%
  filter(!is.na(end_time)) %>%
  mutate(time_gap = difftime(end_time, start_time, units = "secs")) %>%
  mutate(site = "MUDS2") %>%
  select(site, everything()) %>%
  rename(data_status = comment) %>% 
  ungroup() -> muds2_missing_data

muds2_missing_data %>%
  select(-c(gap_number, gap_length)) %>%
  mutate(time_gap = as.period(time_gap)) %>%
  filter(data_status != "Collected") %>%
  write.csv(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/MUDS2_missing_data.csv", row.names = FALSE)


muds2 %>%
  mutate(comment = ifelse(comment == "Did not collect", "Missing", comment)) %>%
  ggplot(aes(x = Date, y = plotid, colour = comment)) +
  geom_point(shape = "|", size = 6) +
  scale_color_manual(values = c("skyblue3", "firebrick1", "black")) +
  scale_x_datetime() + 
  ggtitle("MUDS2", subtitle = "Tile Flow Data (daily)") +
  theme_bw() + 
  guides(colour = guide_legend(override.aes = list(size = 7))) +
  theme(#panel.grid = element_blank(),
    plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(size = 14, hjust = 0.5),
    axis.title = element_blank(),
    axis.text = element_text(size = 12),
    legend.title = element_blank(),
    legend.text = element_text(size = 10),
    legend.key.size = unit(1, "cm")) 
ggsave(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/MUDS2_missing_data.png", width = 8, height = 4)









# MUDS3_OLD ===========================
muds <-  tibble(Time = 0)
read_excel(path = "~/TD/Research Data/MUDS3_OLD/MUDS3_OLD Tile Flow_20170823.xlsx",
           sheet = "2010", n_max = 1) %>% names() -> muds_names
for (i in 2010:2013) {
  read_excel("~/TD/Research Data/MUDS3_OLD/MUDS3_OLD Tile Flow_20170823.xlsx", 
             sheet = as.character(i),
             skip = 2, 
             col_types = c(rep("date", 2), rep("numeric", 4)),
             col_names = muds_names) -> temp
  muds <- bind_rows(muds, temp)
}


muds %>%
  filter(!is.na(Date)) %>%
  select(-Time) %>%
  gather(key = plotid, value = flow, 2:5) %>%
  mutate(plotid = word(plotid, 1)) %>%
  mutate(comment = ifelse(is.na(flow), "Did not collect", "Collected")) %>%
  ungroup() -> muds3_old

muds3_old %>%
  group_by(plotid) %>%
  mutate(gap_length = rep(rle(comment)[[1]], rle(comment)[[1]]),
         gap_number = rep(1:length(rle(comment)[[1]]), rle(comment)[[1]])) %>%
  group_by(plotid, gap_number) %>%
  filter(row_number() == 1 | row_number() == n()) %>%
  mutate(first_last = ifelse(row_number()== 1, "start_date", "end_date")) %>%
  group_by(plotid) %>% 
  mutate(flow = ifelse(first_last == "end_date" & Date < max(Date),
                       "D", "OK")) %>%
  filter(flow == "OK") %>%
  mutate(start_time = Date,
         end_time = lead(Date)) %>% 
  select(-c(flow, first_last, Date)) %>%
  filter(!is.na(end_time)) %>%
  mutate(time_gap = difftime(end_time, start_time, units = "secs")) %>%
  mutate(site = "MUDS3_OLD") %>%
  select(site, everything()) %>%
  rename(data_status = comment) %>% 
  ungroup() -> muds3_old_missing_data

muds3_old_missing_data %>%
  select(-c(gap_number, gap_length)) %>%
  mutate(time_gap = as.period(time_gap)) %>%
  filter(data_status != "Collected") %>%
  write.csv(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/MUDS3_OLD_missing_data.csv", row.names = FALSE)


muds3_old %>%
  mutate(comment = ifelse(comment == "Did not collect", "Missing", comment)) %>%
  ggplot(aes(x = Date, y = plotid, colour = comment)) +
  geom_point(shape = "|", size = 15) +
  scale_color_manual(values = c("skyblue3", "firebrick1", "black")) +
  scale_x_datetime() + 
  ggtitle("MUDS3_OLD", subtitle = "Tile Flow Data (daily)") +
  theme_bw() + 
  guides(colour = guide_legend(override.aes = list(size = 7))) +
  theme(#panel.grid = element_blank(),
    plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(size = 14, hjust = 0.5),
    axis.title = element_blank(),
    axis.text = element_text(size = 12),
    legend.title = element_blank(),
    legend.text = element_text(size = 10),
    legend.key.size = unit(1, "cm")) 
ggsave(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/MUDS3_OLD_missing_data.png", width = 8, height = 4)











# MUDS4 ===========================
muds <-  tibble(Time = 0)
read_excel(path = "~/TD/Research Data/MUDS4/MUDS4 Tile Flow_20170823.xlsx",
           sheet = "2010", n_max = 1) %>% names() -> muds_names
for (i in 2010:2013) {
  read_excel("~/TD/Research Data/MUDS4/MUDS4 Tile Flow_20170823.xlsx", 
             sheet = as.character(i),
             skip = 2, 
             col_types = c(rep("date", 2), rep("numeric", 8)),
             col_names = muds_names) -> temp
  muds <- bind_rows(muds, temp)
}


muds %>%
  filter(!is.na(Date)) %>%
  select(-Time) %>%
  gather(key = plotid, value = flow, 2:9) %>%
  mutate(plotid = word(plotid, 1)) %>%
  mutate(comment = ifelse(is.na(flow), "Did not collect", "Collected")) %>%
  ungroup() -> muds4

muds4 %>%
  group_by(plotid) %>%
  mutate(gap_length = rep(rle(comment)[[1]], rle(comment)[[1]]),
         gap_number = rep(1:length(rle(comment)[[1]]), rle(comment)[[1]])) %>%
  group_by(plotid, gap_number) %>%
  filter(row_number() == 1 | row_number() == n()) %>%
  mutate(first_last = ifelse(row_number()== 1, "start_date", "end_date")) %>%
  group_by(plotid) %>% 
  mutate(flow = ifelse(first_last == "end_date" & Date < max(Date),
                       "D", "OK")) %>%
  filter(flow == "OK") %>%
  mutate(start_time = Date,
         end_time = lead(Date)) %>% 
  select(-c(flow, first_last, Date)) %>%
  filter(!is.na(end_time)) %>%
  mutate(time_gap = difftime(end_time, start_time, units = "secs")) %>%
  mutate(site = "MUDS4") %>%
  select(site, everything()) %>%
  rename(data_status = comment) %>% 
  ungroup() -> muds4_missing_data

muds4_missing_data %>%
  select(-c(gap_number, gap_length)) %>%
  mutate(time_gap = as.period(time_gap)) %>%
  filter(data_status != "Collected") %>%
  write.csv(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/MUDS4_missing_data.csv", row.names = FALSE)


muds4 %>%
  mutate(comment = ifelse(comment == "Did not collect", "Missing", comment)) %>%
  ggplot(aes(x = Date, y = plotid, colour = comment)) +
  geom_point(shape = "|", size = 6) +
  scale_color_manual(values = c("skyblue3", "firebrick1", "black")) +
  scale_x_datetime() + 
  ggtitle("MUDS4", subtitle = "Tile Flow Data (daily)") +
  theme_bw() + 
  guides(colour = guide_legend(override.aes = list(size = 7))) +
  theme(#panel.grid = element_blank(),
    plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(size = 14, hjust = 0.5),
    axis.title = element_blank(),
    axis.text = element_text(size = 12),
    legend.title = element_blank(),
    legend.text = element_text(size = 10),
    legend.key.size = unit(1, "cm")) 
ggsave(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/MUDS4_missing_data.png", width = 8, height = 4)






# TIDE_OLD ==================
tide <-  tibble(Time = 0)
read_excel(path = "~/TD/Research Data/TIDE_OLD/TIDE_OLD Tile Flow_20170823.xlsx",
           sheet = "2007", n_max = 1) %>% names() -> tide_names
for (i in 2007:2011) {
  read_excel("~/TD/Research Data/TIDE_OLD/TIDE_OLD Tile Flow_20170823.xlsx", 
             sheet = as.character(i),
             skip = 2, 
             col_types = c(rep("date", 2), rep("numeric", 4)),
             col_names = tide_names) -> temp
  tide <- bind_rows(tide, temp)
}


tide %>%
  filter(!is.na(Date)) %>%
  select(-Time) %>%
  gather(key = plotid, value = flow, 2:5) %>%
  mutate(plotid = word(plotid, 1)) %>%
  mutate(comment = ifelse(is.na(flow), "Did not collect", "Collected")) %>%
  ungroup() -> tide_old

tide_old %>%
  group_by(plotid) %>%
  mutate(gap_length = rep(rle(comment)[[1]], rle(comment)[[1]]),
         gap_number = rep(1:length(rle(comment)[[1]]), rle(comment)[[1]])) %>%
  group_by(plotid, gap_number) %>%
  filter(row_number() == 1 | row_number() == n()) %>%
  mutate(first_last = ifelse(row_number()== 1, "start_date", "end_date")) %>%
  group_by(plotid) %>% 
  mutate(flow = ifelse(first_last == "end_date" & Date < max(Date),
                       "D", "OK")) %>%
  filter(flow == "OK") %>%
  mutate(start_time = Date,
         end_time = lead(Date)) %>% 
  select(-c(flow, first_last, Date)) %>%
  filter(!is.na(end_time)) %>%
  mutate(time_gap = difftime(end_time, start_time, units = "secs")) %>%
  mutate(site = "TIDE_OLD") %>%
  select(site, everything()) %>%
  rename(data_status = comment) %>% 
  ungroup() -> tide_old_missing_data

tide_old_missing_data %>%
  select(-c(gap_number, gap_length)) %>%
  filter(data_status != "Collected") %>%
  mutate(time_gap = as.period(time_gap)) %>%
  write.csv(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/TIDE_OLD_missing_data.csv", row.names = FALSE)


tide_old %>%
  mutate(comment = ifelse(comment == "Did not collect", "Missing", comment)) %>%
  ggplot(aes(x = Date, y = plotid, colour = comment)) +
  geom_point(shape = "|", size = 15) +
  scale_color_manual(values = c("skyblue3", "firebrick1", "black")) +
  scale_x_datetime() + 
  ggtitle("TIDE_OLD", subtitle = "Tile Flow Data (daily)") +
  theme_bw() + 
  guides(colour = guide_legend(override.aes = list(size = 7))) +
  theme(#panel.grid = element_blank(),
    plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(size = 14, hjust = 0.5),
    axis.title = element_blank(),
    axis.text = element_text(size = 12),
    legend.title = element_blank(),
    legend.text = element_text(size = 10),
    legend.key.size = unit(1, "cm")) 
ggsave(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/TIDE_OLD_missing_data.png", width = 8, height = 4)







# FAIRM ==================
fairm <-  tibble(Time = 0)
read_excel(path = "~/TD/Research Data/FAIRM/FAIRM Tile Flow_20170823.xlsx",
           sheet = "2013", n_max = 1) %>% names() -> fairm_names
for (i in 2013:2016) {
  read_excel("~/TD/Research Data/FAIRM/FAIRM Tile Flow_20170823.xlsx", 
             sheet = as.character(i),
             skip = 2, 
             col_types = c(rep("date", 2), rep("numeric", 2)),
             col_names = fairm_names) -> temp
  fairm <- bind_rows(fairm, temp)
}

d1 <- min(as.Date(fairm$Date), na.rm = T)
dn <- max(as.Date(fairm$Date), na.rm = T)
tibble(Date = seq.Date(from = d1, to = dn, by = "day")) %>%
  mutate(Date = as_datetime(Date)) -> Date


fairm %>%
  filter(!is.na(Date)) %>%
  select(-Time) %>%
  full_join(Date, by = "Date") %>%
  arrange(Date) %>%
  gather(key = plotid, value = flow, 2:3) %>%
  mutate(plotid = word(plotid, 1),
         flow = ifelse(is.na(flow), 0, flow)) %>%
  mutate(comment = ifelse(is.na(flow), "Did not collect", "Collected")) %>%
  ungroup() -> fairmont

fairmont %>%
  group_by(plotid) %>%
  mutate(gap_length = rep(rle(comment)[[1]], rle(comment)[[1]]),
         gap_number = rep(1:length(rle(comment)[[1]]), rle(comment)[[1]])) %>%
  group_by(plotid, gap_number) %>%
  filter(row_number() == 1 | row_number() == n()) %>%
  mutate(first_last = ifelse(row_number()== 1, "start_date", "end_date")) %>%
  group_by(plotid) %>% 
  mutate(flow = ifelse(first_last == "end_date" & Date < max(Date),
                       "D", "OK")) %>%
  filter(flow == "OK") %>%
  mutate(start_time = Date,
         end_time = lead(Date)) %>% 
  select(-c(flow, first_last, Date)) %>%
  filter(!is.na(end_time)) %>%
  mutate(time_gap = difftime(end_time, start_time, units = "secs")) %>%
  mutate(site = "FAIRM") %>%
  select(site, everything()) %>%
  rename(data_status = comment) %>% 
  ungroup() -> fairmont_missing_data

fairmont_missing_data %>%
  select(-c(gap_number, gap_length)) %>%
  mutate(time_gap = as.period(time_gap)) %>%
  filter(data_status != "Collected") %>%
  write.csv(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/FAIRM_missing_data.csv", row.names = FALSE)


fairmont %>%
  mutate(comment = ifelse(comment == "Did not collect", "Missing", comment)) %>%
  ggplot(aes(x = Date, y = plotid, colour = comment)) +
  geom_point(shape = "|", size = 15) +
  scale_color_manual(values = c("skyblue3", "firebrick1", "black")) +
  scale_x_datetime() + 
  ggtitle("FAIRM", subtitle = "Tile Flow Data (daily)") +
  theme_bw() + 
  guides(colour = guide_legend(override.aes = list(size = 7))) +
  theme(#panel.grid = element_blank(),
    plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(size = 14, hjust = 0.5),
    axis.title = element_blank(),
    axis.text = element_text(size = 12),
    legend.title = element_blank(),
    legend.text = element_text(size = 10),
    legend.key.size = unit(1, "cm")) 
ggsave(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/FAIRM_missing_data.png", width = 8, height = 4)









# UBWC ==================
ubwc <-  tibble(Time = 0)
read_excel(path = "~/TD/Research Data/UBWC/UBWC Tile Flow_20170823.xlsx",
           sheet = "2011", n_max = 1) %>% names() -> ubwc_names
for (i in 2005:2012) {
  read_excel("~/TD/Research Data/UBWC/UBWC Tile Flow_20170823.xlsx", 
             sheet = as.character(i),
             skip = 2, 
             col_types = c(rep("date", 2), rep("numeric", 2)),
             col_names = ubwc_names) -> temp
  ubwc <- bind_rows(ubwc, temp)
}

ubwc %>%
  filter(!is.na(Date)) %>%
  select(-Time) %>%
  arrange(Date) %>%
  gather(key = plotid, value = flow, 2:3) %>%
  mutate(plotid = word(plotid, 1)) %>%
  mutate(comment = ifelse(is.na(flow), "Did not collect", "Collected")) %>%
  ungroup() -> ubwc

ubwc %>%
  group_by(plotid) %>%
  mutate(gap_length = rep(rle(comment)[[1]], rle(comment)[[1]]),
         gap_number = rep(1:length(rle(comment)[[1]]), rle(comment)[[1]])) %>%
  group_by(plotid, gap_number) %>%
  filter(row_number() == 1 | row_number() == n()) %>%
  mutate(first_last = ifelse(row_number()== 1, "start_date", "end_date")) %>%
  group_by(plotid) %>% 
  mutate(flow = ifelse(first_last == "end_date" & Date < max(Date),
                       "D", "OK")) %>%
  filter(flow == "OK") %>%
  mutate(start_time = Date,
         end_time = lead(Date)) %>% 
  select(-c(flow, first_last, Date)) %>%
  filter(!is.na(end_time)) %>%
  mutate(time_gap = difftime(end_time, start_time, units = "secs")) %>%
  mutate(site = "UBWC") %>%
  select(site, everything()) %>%
  rename(data_status = comment) %>% 
  ungroup() -> ubwc_missing_data

ubwc_missing_data %>%
  select(-c(gap_number, gap_length)) %>%
  mutate(time_gap = as.period(time_gap)) %>%
  filter(data_status != "Collected") %>%
  write.csv(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/UBWC_missing_data.csv", row.names = FALSE)


ubwc %>%
  mutate(comment = ifelse(comment == "Did not collect", "Missing", comment)) %>%
  ggplot(aes(x = Date, y = plotid, colour = comment)) +
  geom_point(shape = "|", size = 15) +
  scale_color_manual(values = c("skyblue3", "firebrick1", "black")) +
  scale_x_datetime() + 
  ggtitle("UBWC", subtitle = "Tile Flow Data (daily)") +
  theme_bw() + 
  guides(colour = guide_legend(override.aes = list(size = 7))) +
  theme(#panel.grid = element_blank(),
    plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(size = 14, hjust = 0.5),
    axis.title = element_blank(),
    axis.text = element_text(size = 12),
    legend.title = element_blank(),
    legend.text = element_text(size = 10),
    legend.key.size = unit(1, "cm")) 
ggsave(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/UBWC_missing_data.png", width = 8, height = 4)









# DEFI_M ================
defi <-  tibble(Time = 0)
read_excel(path = "~/TD/Research Data/DEFI_M/DEFI_M Tile Flow_20170823.xlsx",
           sheet = "2011", n_max = 1) %>% names() -> defi_names
for (i in 2008:2014) {
  read_excel("~/TD/Research Data/DEFI_M/DEFI_M Tile Flow_20170823.xlsx", 
             sheet = as.character(i),
             skip = 2, 
             col_types = c(rep("date", 2), rep("numeric", 2)),
             col_names = defi_names) -> temp
  defi <- bind_rows(defi, temp)
}

defi %>%
  filter(!is.na(Date)) %>%
  mutate(Time = as_datetime(Time)) %>%
  mutate(Date = ifelse(is.na(Time), Date, 
                       ymd_hm(paste(Date, format(Time, "%H:%M")))),
         Date = as_datetime(Date)) %>%
  select(-Time) %>%
  gather(key = plotid, value = flow, 2:3) %>%
  mutate(plotid = word(plotid, 1)) %>%
  arrange(plotid, Date) %>%
  mutate(flow = ifelse(flow < 0, NA, flow)) %>%
  mutate(comment = ifelse(is.na(flow), "Did not collect", "Collected")) %>%
  mutate(Date = round_date(Date, unit = "minute")) %>%
  ungroup() -> defi_m

defi_m %>%
  group_by(plotid) %>%
  mutate(gap_length = rep(rle(comment)[[1]], rle(comment)[[1]]),
         gap_number = rep(1:length(rle(comment)[[1]]), rle(comment)[[1]])) %>%
  group_by(plotid, gap_number) %>%
  filter(row_number() == 1 | row_number() == n()) %>%
  mutate(first_last = ifelse(row_number()== 1, "start_date", "end_date")) %>%
  group_by(plotid) %>% 
  mutate(flow = ifelse(first_last == "end_date" & Date < max(Date),
                       "D", "OK")) %>%
  filter(flow == "OK") %>%
  mutate(start_time = Date,
         end_time = lead(Date)) %>% 
  select(-c(flow, first_last, Date)) %>%
  filter(!is.na(end_time)) %>%
  mutate(time_gap = difftime(end_time, start_time, units = "secs")) %>%
  mutate(site = "DEFI_M") %>%
  select(site, everything()) %>%
  rename(data_status = comment) %>% 
  ungroup() -> defi_m_missing_data

defi_m_missing_data %>%
  select(-c(gap_number, gap_length)) %>%
  filter(data_status != "Collected") %>%
  mutate(time_gap = as.period(time_gap)) %>%
  write.csv(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/DEFI_M_missing_data.csv", row.names = FALSE)


defi_m %>%
  mutate(comment = ifelse(comment == "Did not collect", "Missing", comment)) %>%
  ggplot(aes(x = Date, y = plotid, colour = comment)) +
  geom_point(shape = "|", size = 15) +
  scale_color_manual(values = c("skyblue3", "firebrick1", "black")) +
  scale_x_datetime() + 
  ggtitle("DEFI_M", subtitle = "Tile Flow Data (daily 2008-2014, irregular sub-daily 2010)") +
  theme_bw() + 
  guides(colour = guide_legend(override.aes = list(size = 7))) +
  theme(#panel.grid = element_blank(),
    plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(size = 14, hjust = 0.5),
    axis.title = element_blank(),
    axis.text = element_text(size = 12),
    legend.title = element_blank(),
    legend.text = element_text(size = 10),
    legend.key.size = unit(1, "cm")) 
ggsave(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/DEFI_M_missing_data.png", width = 8, height = 4)








# HARDIN ================
hard <-  tibble(Time = 0)
read_excel(path = "~/TD/Research Data/HARDIN/HARDIN Tile Flow_20170823.xlsx",
           sheet = "2011", n_max = 1) %>% names() -> hard_names
for (i in 2009:2014) {
  read_excel("~/TD/Research Data/HARDIN/HARDIN Tile Flow_20170823.xlsx", 
             sheet = as.character(i),
             skip = 2, 
             col_types = c(rep("date", 2), rep("numeric", 2)),
             col_names = hard_names) -> temp
  hard <- bind_rows(hard, temp)
}

hard %>%
  filter(!is.na(Date)) %>%
  mutate(Time = as_datetime(Time)) %>%
  mutate(Date = ifelse(year(Date) != 2009, Date, 
                       ymd_hm(paste(Date, format(Time, "%H:%M")))),
         Date = as_datetime(Date)) %>%
  select(-Time) %>%
  gather(key = plotid, value = flow, 2:3) %>%
  mutate(plotid = word(plotid, 1)) %>%
  arrange(plotid, Date) %>%
  mutate(flow = ifelse(flow < 0, NA, flow)) %>%
  mutate(comment = ifelse(is.na(flow), "Did not collect", "Collected")) %>%
  mutate(Date = round_date(Date, unit = "minute")) %>%
  ungroup() -> hardin

hardin %>%
  group_by(plotid) %>%
  mutate(gap_length = rep(rle(comment)[[1]], rle(comment)[[1]]),
         gap_number = rep(1:length(rle(comment)[[1]]), rle(comment)[[1]])) %>%
  group_by(plotid, gap_number) %>%
  filter(row_number() == 1 | row_number() == n()) %>%
  mutate(first_last = ifelse(row_number()== 1, "start_date", "end_date")) %>%
  group_by(plotid) %>% 
  mutate(flow = ifelse(first_last == "end_date" & Date < max(Date),
                       "D", "OK")) %>%
  filter(flow == "OK") %>%
  mutate(start_time = Date,
         end_time = lead(Date)) %>% 
  select(-c(flow, first_last, Date)) %>%
  filter(!is.na(end_time)) %>%
  mutate(time_gap = difftime(end_time, start_time, units = "secs")) %>%
  mutate(site = "HARDIN") %>%
  select(site, everything()) %>%
  rename(data_status = comment) %>% 
  ungroup() -> hardin_missing_data

hardin_missing_data %>%
  select(-c(gap_number, gap_length)) %>%
  filter(data_status != "Collected") %>%
  mutate(time_gap = as.period(time_gap)) %>%
  write.csv(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/HARDIN_missing_data.csv", row.names = FALSE)


hardin %>%
  mutate(comment = ifelse(comment == "Did not collect", "Missing", comment)) %>%
  ggplot(aes(x = Date, y = plotid, colour = comment)) +
  geom_point(shape = "|", size = 15) +
  scale_color_manual(values = c("skyblue3", "firebrick1", "black")) +
  scale_x_datetime() + 
  ggtitle("HARDIN", subtitle = "Tile Flow Data (hourly 2009, daily 2011-2014)") +
  theme_bw() + 
  guides(colour = guide_legend(override.aes = list(size = 7))) +
  theme(#panel.grid = element_blank(),
    plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(size = 14, hjust = 0.5),
    axis.title = element_blank(),
    axis.text = element_text(size = 12),
    legend.title = element_blank(),
    legend.text = element_text(size = 10),
    legend.key.size = unit(1, "cm")) 
ggsave(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/HARDIN_missing_data.png", width = 8, height = 4)





# HARDIN_NW ==================
hard <-  tibble(Time = 0)
read_excel(path = "~/TD/Research Data/HARDIN_NW/HARDIN_NW Tile Flow_20170823.xlsx",
           sheet = "2011", n_max = 1) %>% names() -> hard_names
for (i in 2011:2014) {
  read_excel("~/TD/Research Data/HARDIN_NW/HARDIN_NW Tile Flow_20170823.xlsx", 
             sheet = as.character(i),
             skip = 2, 
             col_types = c(rep("date", 2), rep("numeric", 2)),
             col_names = hard_names) -> temp
  hard <- bind_rows(hard, temp)
}

hard %>%
  filter(!is.na(Date)) %>%
  select(-Time) %>%
  arrange(Date) %>%
  gather(key = plotid, value = flow, 2:3) %>%
  mutate(plotid = word(plotid, 1)) %>%
  mutate(comment = ifelse(is.na(flow), "Did not collect", "Collected")) %>%
  ungroup() -> hardin_nw

hardin_nw %>%
  group_by(plotid) %>%
  mutate(gap_length = rep(rle(comment)[[1]], rle(comment)[[1]]),
         gap_number = rep(1:length(rle(comment)[[1]]), rle(comment)[[1]])) %>%
  group_by(plotid, gap_number) %>%
  filter(row_number() == 1 | row_number() == n()) %>%
  mutate(first_last = ifelse(row_number()== 1, "start_date", "end_date")) %>%
  group_by(plotid) %>% 
  mutate(flow = ifelse(first_last == "end_date" & Date < max(Date),
                       "D", "OK")) %>%
  filter(flow == "OK") %>%
  mutate(start_time = Date,
         end_time = lead(Date)) %>% 
  select(-c(flow, first_last, Date)) %>%
  filter(!is.na(end_time)) %>%
  mutate(time_gap = difftime(end_time, start_time, units = "secs")) %>%
  mutate(site = "HARDIN_NW") %>%
  select(site, everything()) %>%
  rename(data_status = comment) %>% 
  ungroup() -> hardin_nw_missing_data

hardin_nw_missing_data %>%
  select(-c(gap_number, gap_length)) %>%
  mutate(time_gap = as.period(time_gap)) %>%
  filter(data_status != "Collected") %>%
  write.csv(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/HARDIN_NW_missing_data.csv", row.names = FALSE)


hardin_nw %>%
  mutate(comment = ifelse(comment == "Did not collect", "Missing", comment)) %>%
  ggplot(aes(x = Date, y = plotid, colour = comment)) +
  geom_point(shape = "|", size = 15) +
  scale_color_manual(values = c("skyblue3", "firebrick1", "black")) +
  scale_x_datetime() + 
  ggtitle("HARDIN_NW", subtitle = "Tile Flow Data (daily)") +
  theme_bw() + 
  guides(colour = guide_legend(override.aes = list(size = 7))) +
  theme(#panel.grid = element_blank(),
    plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(size = 14, hjust = 0.5),
    axis.title = element_blank(),
    axis.text = element_text(size = 12),
    legend.title = element_blank(),
    legend.text = element_text(size = 10),
    legend.key.size = unit(1, "cm")) 
ggsave(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/HARDIN_NW_missing_data.png", width = 8, height = 4)




# HENRY ==================
henry <-  tibble(Time = 0)
read_excel(path = "~/TD/Research Data/HENRY/HENRY Tile Flow_20170823.xlsx",
           sheet = "2011", n_max = 1) %>% names() -> henry_names
for (i in 2011:2014) {
  read_excel("~/TD/Research Data/HENRY/HENRY Tile Flow_20170823.xlsx", 
             sheet = as.character(i),
             skip = 2, 
             col_types = c(rep("date", 2), rep("numeric", 2)),
             col_names = henry_names) -> temp
  henry <- bind_rows(henry, temp)
}

henry %>%
  filter(!is.na(Date)) %>%
  select(-Time) %>%
  arrange(Date) %>%
  gather(key = plotid, value = flow, 2:3) %>%
  mutate(plotid = word(plotid, 1)) %>%
  filter(!is.na(flow)) %>%
  group_by(plotid, Date) %>%
  summarise_all(funs(sum)) %>%
  spread(key = plotid, value = flow) %>%
  gather(key = plotid, value = flow, East:West) %>%
  mutate(comment = ifelse(is.na(flow), "Did not collect", "Collected")) %>%
  ungroup() -> henry

henry %>%
  group_by(plotid) %>%
  mutate(gap_length = rep(rle(comment)[[1]], rle(comment)[[1]]),
         gap_number = rep(1:length(rle(comment)[[1]]), rle(comment)[[1]])) %>%
  group_by(plotid, gap_number) %>%
  filter(row_number() == 1 | row_number() == n()) %>%
  mutate(first_last = ifelse(row_number()== 1, "start_date", "end_date")) %>%
  group_by(plotid) %>% 
  mutate(flow = ifelse(first_last == "end_date" & Date < max(Date),
                       "D", "OK")) %>%
  filter(flow == "OK") %>%
  mutate(start_time = Date,
         end_time = lead(Date)) %>% 
  select(-c(flow, first_last, Date)) %>%
  filter(!is.na(end_time)) %>%
  mutate(time_gap = difftime(end_time, start_time, units = "secs")) %>%
  mutate(site = "HENRY") %>%
  select(site, everything()) %>%
  rename(data_status = comment) %>% 
  ungroup() -> henry_missing_data

henry_missing_data %>%
  select(-c(gap_number, gap_length)) %>%
  mutate(time_gap = as.period(time_gap)) %>%
  filter(data_status != "Collected") %>%
  write.csv(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/HENRY_missing_data.csv", row.names = FALSE)


henry %>%
  mutate(comment = ifelse(comment == "Did not collect", "Missing", comment)) %>%
  ggplot(aes(x = Date, y = plotid, colour = comment)) +
  geom_point(shape = "|", size = 15) +
  scale_color_manual(values = c("skyblue3", "firebrick1", "black")) +
  scale_x_datetime() + 
  ggtitle("HENRY", subtitle = "Tile Flow Data (other 2011-2012, daily 2013-2014)") +
  theme_bw() + 
  guides(colour = guide_legend(override.aes = list(size = 7))) +
  theme(#panel.grid = element_blank(),
    plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(size = 14, hjust = 0.5),
    axis.title = element_blank(),
    axis.text = element_text(size = 12),
    legend.title = element_blank(),
    legend.text = element_text(size = 10),
    legend.key.size = unit(1, "cm")) 
ggsave(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/HENRY_missing_data.png", width = 8, height = 4)







# CRAWF ======
crawf <-  tibble(Time = 0)
read_excel(path = "~/TD/Research Data/CRAWF/CRAWF Tile Flow_20170823.xlsx",
           sheet = "2011", n_max = 1) %>% names() -> crawf_names
for (i in 2008:2014) {
  read_excel("~/TD/Research Data/CRAWF/CRAWF Tile Flow_20170823.xlsx", 
             sheet = as.character(i),
             skip = 2, 
             col_types = c(rep("date", 2), rep("numeric", 2)),
             col_names = crawf_names) -> temp
  crawf <- bind_rows(crawf, temp)
}

crawf %>%
  filter(!is.na(Date)) %>%
  mutate(Time = as_datetime(Time)) %>%
  mutate(Date = ymd_hm(paste(Date, format(Time, "%H:%M"))),
         Date = as_datetime(Date)) %>%
  mutate(Time = as_datetime(floor(as.numeric(Date)/1800)*1800)) %>% 
  gather(key = plotid, value = flow, 3:4) %>%
  mutate(plotid = word(plotid, 1)) %>%
  filter(!is.na(flow)) %>%
  mutate(Time = ifelse(year(Date) > 2008, row_number(), Time)) %>%
  group_by(Time, plotid) %>%
  summarise(flow = sum(flow),
            Date = first(Date)) %>%
  arrange(plotid, Date) %>%
  ungroup() %>%
  select(Date, plotid, flow) -> crawf_2

crawf_2 %>%
  spread(key = plotid, value = flow) %>%
  gather(key = plotid, value = flow, North:South) %>%
  mutate(comment = ifelse(is.na(flow), "Did not collect", "Collected")) %>%
  ungroup() -> crawf_3

crawf_3 %>%
  group_by(plotid) %>%
  mutate(gap_length = rep(rle(comment)[[1]], rle(comment)[[1]]),
         gap_number = rep(1:length(rle(comment)[[1]]), rle(comment)[[1]])) %>%
  group_by(plotid, gap_number) %>%
  filter(row_number() == 1 | row_number() == n()) %>%
  mutate(first_last = ifelse(row_number()== 1, "start_date", "end_date")) %>%
  group_by(plotid) %>% 
  mutate(flow = ifelse(first_last == "end_date" & Date < max(Date),
                       "D", "OK")) %>%
  filter(flow == "OK") %>%
  mutate(start_time = Date,
         end_time = lead(Date)) %>% 
  select(-c(flow, first_last, Date)) %>%
  filter(!is.na(end_time)) %>%
  mutate(time_gap = difftime(end_time, start_time, units = "secs")) %>%
  mutate(site = "CRAWF") %>%
  select(site, everything()) %>%
  rename(data_status = comment) %>% 
  ungroup() -> crawf_missing_data

crawf_missing_data %>%
  select(-c(gap_number, gap_length)) %>%
  mutate(time_gap = as.period(time_gap)) %>%
  filter(data_status != "Collected") %>%
  write.csv(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/CRAWF_missing_data.csv", row.names = FALSE)


crawf_3 %>%
  mutate(comment = ifelse(comment == "Did not collect", "Missing", comment)) %>%
  ggplot(aes(x = Date, y = plotid, colour = comment)) +
  geom_point(shape = "|", size = 15) +
  scale_color_manual(values = c("skyblue3", "firebrick1", "black")) +
  scale_x_datetime() + 
  ggtitle("CRAWF", subtitle = "Tile Flow Data") +
  theme_bw() + 
  guides(colour = guide_legend(override.aes = list(size = 7))) +
  theme(#panel.grid = element_blank(),
    plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(size = 14, hjust = 0.5),
    axis.title = element_blank(),
    axis.text = element_text(size = 12),
    legend.title = element_blank(),
    legend.text = element_text(size = 10),
    legend.key.size = unit(1, "cm")) 
ggsave(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/CRAWF_missing_data.png", width = 8, height = 4)








# AUGLA ==================
augla <-  tibble(Time = 0)
read_excel(path = "~/TD/Research Data/AUGLA/AUGLA Tile Flow_20170823.xlsx",
           sheet = "2011", n_max = 1) %>% names() -> augla_names
for (i in 2008:2014) {
  read_excel("~/TD/Research Data/AUGLA/AUGLA Tile Flow_20170823.xlsx", 
             sheet = as.character(i),
             skip = 2, 
             col_types = c(rep("date", 2), rep("numeric", 2)),
             col_names = augla_names) -> temp
  augla <- bind_rows(augla, temp)
}

augla %>%
  filter(!is.na(Date)) %>%
  mutate(Time = ifelse(is.na(Time),0, Time), 
         Time = as_datetime(Time)) %>%
  mutate(Date = ymd_hm(paste(Date, format(Time, "%H:%M"))),
         Date = as_datetime(Date),
         Year = year(Date)) %>%
  filter(!(Year == 2012 & is.na(`East WAT1 Tile Flow`))) %>%
  filter(!(Year == 2013 & Date < as_datetime("2013-02-22 08:00:00") &
           is.na(`East WAT1 Tile Flow`))) %>%
  mutate(Minutes = minute(Date)) %>%
  filter(!(Date > as_datetime("2013-02-22 08:00:00") &
           Minutes %in% c(5, 10, 20, 25, 35, 40, 50, 55))) %>%
  rename(flow = `East WAT1 Tile Flow`) %>%
  mutate(plotid = "East") %>%
  select(Time, Date, plotid, flow) -> augla_East


augla %>%
  filter(!is.na(Date)) %>%
  mutate(Time = ifelse(is.na(Time),0, Time), 
         Time = as_datetime(Time)) %>%
  mutate(Date = ymd_hm(paste(Date, format(Time, "%H:%M"))),
         Date = as_datetime(Date),
         Year = year(Date)) %>%
  filter(!(Year == 2012 & !is.na(`East WAT1 Tile Flow`))) %>%
  filter(!(Year == 2012 & is.na(`West WAT1 Tile Flow`) & minute(Date) == 18)) %>%
  filter(!(Year == 2013 & Date < as_datetime("2013-02-22 08:00:00") & 
           is.na(`West WAT1 Tile Flow`))) %>%
  rename(flow = `West WAT1 Tile Flow`) %>%
  mutate(plotid = "West") %>%
  select(Time, Date, plotid, flow) -> augla_West



augla_East %>%
  mutate(Year = year(Date),
         DATE = as.Date(Date)) %>%
  mutate(groupme = ifelse(DATE < as.Date("2013-02-23"), DATE, Date)) %>%
  filter(!(is.na(flow) & DATE < as.Date("2013-02-23") & Year > 2010)) %>%
  group_by(groupme) %>%
  summarise(plotid = first(plotid),
            DATE = first(DATE),
            Date = first(Date),
            flow = sum(flow)) %>%
  ungroup() %>%
  mutate(DATE = as_datetime(DATE),
         Date = ifelse(DATE < as.Date("2013-02-23"),
                       DATE, Date),
         Date = as_datetime(Date)) %>% 
  select(Date, plotid, flow) %>%
  mutate(Date = round_date(Date, unit = "minute")) %>%
  ungroup() -> augla_East_clean



augla_West %>%
  mutate(Year = year(Date),
         DATE = as.Date(Date)) %>%
  mutate(groupme = ifelse(Year == 2011, DATE, Date)) %>%
  filter(!(is.na(flow) & Year == 2011 & DATE < as.Date("2011-05-19"))) %>%
  group_by(groupme) %>%
  summarise(plotid = first(plotid),
            Year = first(Year),
            DATE = first(DATE),
            Date = first(Date),
            flow = sum(flow)) %>%
  arrange(Date) %>%
  ungroup() %>%
  mutate(DATE = as_datetime(DATE),
         Date = ifelse(Year < 2012, DATE, Date),
         Date = as_datetime(Date)) %>% 
  select(Date, plotid, flow) %>%
  mutate(Date = round_date(Date, unit = "minute")) %>%
  ungroup() -> augla_West_clean
  

tibble(Date = seq.Date(from = as.Date("2012-01-01"), 
                       to = as.Date("2012-12-31"), 
                       by = "day")) %>% 
  mutate(Date = as_datetime(Date)) %>% 
  full_join(augla_West_clean, by = "Date") %>%
  mutate(plotid = "West") %>%
  bind_rows(augla_East_clean) %>%
  mutate(comment = ifelse(is.na(flow), "Did not collect", "Collected")) %>%
  arrange(plotid, Date) -> augla_FINAL 


augla_FINAL %>%
  group_by(plotid) %>%
  mutate(gap_length = rep(rle(comment)[[1]], rle(comment)[[1]]),
         gap_number = rep(1:length(rle(comment)[[1]]), rle(comment)[[1]])) %>%
  group_by(plotid, gap_number) %>%
  filter(row_number() == 1 | row_number() == n()) %>%
  mutate(first_last = ifelse(row_number()== 1, "start_date", "end_date")) %>%
  group_by(plotid) %>% 
  mutate(flow = ifelse(first_last == "end_date" & Date < max(Date),
                       "D", "OK")) %>%
  filter(flow == "OK") %>%
  mutate(start_time = Date,
         end_time = lead(Date)) %>% 
  select(-c(flow, first_last, Date)) %>%
  filter(!is.na(end_time)) %>%
  mutate(time_gap = difftime(end_time, start_time, units = "secs")) %>%
  mutate(site = "AUGLA") %>%
  select(site, everything()) %>%
  rename(data_status = comment) %>% 
  ungroup() -> augla_missing_data

augla_missing_data %>%
  select(-c(gap_number, gap_length)) %>%
  mutate(time_gap = as.period(time_gap)) %>%
  filter(data_status != "Collected") %>%
  write.csv(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/AUGLA_missing_data.csv", row.names = FALSE)


augla_FINAL %>%
  mutate(comment = ifelse(comment == "Did not collect", "Missing", comment)) %>%
  ggplot(aes(x = Date, y = plotid, colour = comment)) +
  geom_point(shape = "|", size = 15) +
  scale_color_manual(values = c("skyblue3", "firebrick1", "black")) +
  scale_x_datetime() + 
  ggtitle("AUGLA", subtitle = "Tile Flow Data (daily 2008-2010, other 2011-2012, hourly 2013-2014)") +
  theme_bw() + 
  guides(colour = guide_legend(override.aes = list(size = 7))) +
  theme(#panel.grid = element_blank(),
    plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
    plot.subtitle = element_text(size = 14, hjust = 0.5),
    axis.title = element_blank(),
    axis.text = element_text(size = 12),
    legend.title = element_blank(),
    legend.text = element_text(size = 10),
    legend.key.size = unit(1, "cm")) 
ggsave(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/AUGLA_missing_data.png", width = 8, height = 4)







# PART >>> 2 -------------------------------
ALL <- list(AUGLA = augla_FINAL, 
            CRAWF = crawf_3, 
            DEFI_M =defi_m, 
            DPAC = dpac, 
            FAIRM = fairmont, 
            HARDIN = hardin, 
            HARDIN_NW = hardin_nw,
            HENRY = henry, 
            HICKS_B = hicks_b, 
            MUDS2 = muds2, 
            MUDS3_OLD = muds3_old, 
            MUDS4 = muds4, 
            SERF_IA = serf_ia, 
            SERF_SD = serf_sd,
            STJOHNS = stjohns, 
            STORY = story, 
            TIDE_OLD = tide_old, 
            UBWC = ubwc) %>%
  map2_df(names(.), ~ mutate(.x, site = .y)) %>%
  mutate(date = as.Date(Date),
         comment = NULL) %>%
  select(site, plotid, date, flow)

# Find "empty date gaps"
ALL %>%
  group_by(site, plotid) %>%
  count(date) %>%
  select(-n) %>%
  



# Plot timeline of all sites =======
ALL %>%
  group_by(site, plotid) %>%
  filter(!is.na(flow)) %>%
  count(date) %>%
  select(-n) %>%
  group_by(site) %>%
  count(date) %>%
  ggplot(aes(x=date, y=site)) + 
  geom_point(size = 1, shape = "|", colour = "skyblue4") +
  scale_x_date(date_breaks = "2 year",
               # date_minor_breaks = "6 months",
               date_labels = "%Y") +
  theme_light() +
  theme(axis.title = element_blank(),
        axis.text.x = element_text(hjust = -0.5),
        panel.grid.minor = element_line(colour = "gray"))
ggsave(file = "~/TD/Research Data/_Missing_Data/Flow/missing_data_ALL.png", 
       width = 8, height = 5)


# PART >>> 3 -------------------------------

# Merge all missing files ======================
mget(ls(pattern = "missing")) %>%
  bind_rows() %>% 
  filter(data_status != "Collected") %>%
  mutate(gap = difftime(end_time, start_time),
         time_gap = as.period(gap)) %>%
  select(-c(gap_number, gap_length, gap)) %>%
  write.csv(file = "C:/Users/gio/Documents/TD/Research Data/_Missing_Data/Flow/missing_data.csv", row.names = FALSE)


mget(ls(pattern = "missing")) %>%
  bind_rows() %>% 
  filter(data_status != "Collected") %>%
  mutate(gap_min = difftime(end_time, start_time, units = "min"),
         gap_hour = difftime(end_time, start_time, units = "hour"),
         gap_day = difftime(end_time, start_time, units = "day")) %>%
  select(-c(gap_number, gap_length, start_time, end_time)) %>%
  mutate(hour = gap_hour <= 1,
         hday = gap_hour <= 12,
         day = gap_day <= 1) %>%
  group_by(site) %>%
  summarise(`<= 1 hr` = sum(hour)/n(),
            `<= 12 hr` = sum(hday)/n(),
            `<= 24 hr` = sum(day)/n(),
            `n_of_gaps` = n()) %>%
  gather(key = duration, value = percentage, 2:4) -> missing_percent

missing_percent %>%
  mutate(percentage = percent(percentage)) %>%
  spread(key=duration, value = percentage) %>%
  rename(`Total gaps` = n_of_gaps, 
         `1h or less` = `<= 1 hr`,
         `12h or less` = `<= 12 hr`, 
         `24h or less` = `<= 24 hr`) 

# Plotting missing data summary ==================
missing_percent %>%
  ggplot(aes(x=site, y=percentage)) +
  geom_bar(stat = "identity", fill = "skyblue3") +
  facet_grid(duration~.) +
  scale_y_continuous("Entries Missing, %", labels = percent) +
  theme_light() +
  theme(strip.text = element_text(size = 12, face = "bold"),
        axis.title.x = element_blank())
ggsave(file = "~/TD/Research Data/_Missing_Data/Flow/missing_data.png", 
       width = 8, height = 4)


ggplot(data = missing_percent %>% filter(duration == "<= 24 hr")) +
  geom_bar(aes(x=site, y=percentage), stat = "identity", 
           fill = "skyblue3", alpha = 0.2) +
  geom_bar(data = missing_percent %>% filter(duration == "<= 12 hr"),
           aes(x=site, y=percentage), stat = "identity", 
           fill = "skyblue3", alpha = 0.3) +
  geom_bar(data = missing_percent %>% filter(duration == "<= 1 hr"),
           aes(x=site, y=percentage), stat = "identity", 
           fill = "skyblue4", alpha = 0.5) +
  geom_text(aes(x=site, y=percentage, label=n_of_gaps), vjust = -0.3, size = 5) +
  scale_y_continuous("Entries Missing, %", labels = percent) +
  theme_light() +
  theme(panel.grid.major.x = element_blank(),
        axis.title.x = element_blank(),
        axis.title.y = element_text(size = 14),
        axis.text = element_text(size = 10))
ggsave(file = "~/TD/Research Data/_Missing_Data/Flow/missing_data_2.png", 
       width = 8, height = 5)



googlesheets::gs_title("Data Overview") %>%
  googlesheets::gs_read(ws = "Data Overview (DATA-YEARS)", 
                        range = cell_cols(1:10), 
                        skip =1) %>%
  filter(`DWM Practice` == "CD") -> cd_sites
  
cd_sites %>%
  filter(`DWM Practice` == "CD") %>%
  select(`Site ID`, `US State`, `Tile Flow`) %>%
  rename(site = `Site ID`, state = `US State`, flow = `Tile Flow`) %>%
  mutate(flow = as.integer(flow),
         flow = ifelse(is.na(flow), 0, flow)) %>%
  arrange(site) %>%
  mutate(flow_hour = c(2,0,7,0,5,0,1,0,0,5,0,0,0,10,2,7,0,0,0),
         flow_daily = c(3,0,0,6,0,4,3,4,2,5,4,4,4,0,0,0,4,5,8),
         flow_other = flow - flow_daily - flow_hour) -> plot_sites

plot_sites %>%
  ggplot(aes(x=reorder(site, -flow), y=flow)) +
  geom_bar(stat = "identity", fill = "skyblue3") +
  scale_y_continuous(name = "Number of Available Tile Flow Years ",
                     breaks = seq(0, 10, 2)) +
  theme_light() +
  theme(panel.grid.major.x = element_blank(),
        panel.grid.minor.y = element_blank(),
        axis.title.x = element_blank(),
        axis.title.y = element_text(size = 14),
        axis.text.x = element_text(size = 14, angle = 90, hjust = 1),
        axis.text.y = element_text(size = 12),
        axis.ticks.x = element_blank())
ggsave(file = "~/TD/Research Data/_Missing_Data/Flow/available_data.png", 
       width = 8, height = 5)  



plot_sites %>%
  select(-flow) %>%
  gather(key = frequency, value = years, 3:5) %>%
  mutate(frequency = factor(x = frequency, 
                            levels = c("flow_hour", "flow_daily", "flow_other"),
                            labels = c("Hourly", "Daily", "Other"))) %>%
  ggplot(aes(x=reorder(site, -years, FUN = sum), y=years, fill = frequency)) +
  geom_bar(stat = "identity", position = position_stack(reverse = TRUE)) +
  scale_y_continuous(name = "Number of Available Tile Flow Years ",
                     breaks = seq(0, 10, 2)) +
  scale_fill_manual(name = "Frequency", 
                    values = c("#b2df8a", "skyblue3","#a6cee3", "#1f78b4", "#fdc086")) +
  theme_light() +
  theme(panel.grid.major.x = element_blank(),
        panel.grid.minor.y = element_blank(),
        axis.title.x = element_blank(),
        axis.title.y = element_text(size = 14),
        axis.text.x = element_text(size = 14, angle = 90, hjust = 1),
        axis.text.y = element_text(size = 12),
        axis.ticks.x = element_blank(),
        legend.title = element_text(size = 14),
        legend.text = element_text(size = 12),
        legend.positio = c(0.8, 0.8))
ggsave(file = "~/TD/Research Data/_Missing_Data/Flow/available_data_2.png", 
       width = 8, height = 5) 



# PART >>> 4 ===============================
library(xlsx)
newdir <- "C:/Users/Gio/Documents/TD/Research Data/_Missing_Data/Flow/new_gaps/"

# Function to find all gaps in the data
gapfinder <- function(x){
  x %>%
    mutate(comment = ifelse(!is.na(flow), "Collected", "Missing")) %>%
    group_by(plotid) %>%
    mutate(gap_length = rep(rle(comment)[[1]], rle(comment)[[1]]),
           gap_number = rep(1:length(rle(comment)[[1]]), rle(comment)[[1]])) %>%
    group_by(plotid, gap_number) %>%
    filter(row_number() == 1 | row_number() == n()) %>%
    mutate(first_last = ifelse(row_number()== 1, "start_date", "end_date")) %>%
    group_by(plotid) %>% 
    mutate(flow = ifelse(first_last == "end_date" & Date < max(Date),
                         "D", "OK")) %>%
    filter(flow == "OK") %>%
    mutate(start_time = Date,
           end_time = lead(Date)) %>% 
    select(-c(flow, first_last, Date)) %>%
    filter(!is.na(end_time)) %>%
    mutate(time_gap = difftime(end_time, start_time, units = "secs")) %>%
    select(-c(gap_number, gap_length)) %>%
    mutate(duration = as.character(as.period(time_gap)),
           duration = str_replace(duration, " 0S", "")) %>%
    filter(comment != "Collected") %>%
    select(-comment) %>%
    ungroup() -> x_miss
  return(x_miss)
}

# function for plotting missing data
gapplot <- function(x){
  nplots <- x %>%
    group_by(plotid) %>%
    count() %>%
    nrow()
  
  point_size <- ifelse(nplots < 6, 15, 
                       ifelse(nplots < 8, 10, 6))
  
  x %>% 
    mutate(comment = ifelse(is.na(flow), "Missing", "Collected")) %>%
    ggplot(aes(x = Date, y = plotid, colour = comment)) +
    geom_point(shape = "|", size = point_size) +
    scale_color_manual(values = c("skyblue3", "firebrick1"),
                       labels = function(x) str_wrap(x, width = 12)) +
    scale_x_datetime(labels = date_format("%m-%Y")) + 
    theme_bw() + 
    guides(colour = guide_legend(override.aes = list(size = 7))) +
    theme(plot.title = element_text(size = 16, hjust = 0.5, face = "bold"),
          plot.subtitle = element_text(size = 14, hjust = 0.5),
          axis.title = element_blank(),
          axis.text = element_text(size = 12),
          legend.title = element_blank(),
          legend.text = element_text(size = 10),
          legend.key.size = unit(1, "cm"))
}

#AUGLA ===========
augla %>%
  filter(!is.na(Date)) %>%
  mutate(Time = ifelse(is.na(Time),0, Time), 
         Time = as_datetime(Time)) %>%
  mutate(Date = ymd_hm(paste(Date, format(Time, "%H:%M"))),
         Date = as_datetime(Date),
         Year = year(Date)) %>%
  rename(West = `West WAT1 Tile Flow`, East =`East WAT1 Tile Flow`) %>%
  filter(Year < 2011) -> augla_2008_2010

augla %>%
  filter(!is.na(Date)) %>%
  mutate(Time = ifelse(is.na(Time),0, Time), 
         Time = as_datetime(Time)) %>%
  mutate(Date = ymd_hm(paste(Date, format(Time, "%H:%M"))),
         Date = as_datetime(Date),
         Year = year(Date)) %>%
  rename(West = `West WAT1 Tile Flow`, East =`East WAT1 Tile Flow`) %>%
  filter(Year > 2010) -> aauugla

aauugla %>%
  
  


x %>%
  filter(!(Year == 2012 & is.na(`East WAT1 Tile Flow`))) %>%
  filter(!(Year == 2013 & Date < as_datetime("2013-02-22 08:00:00") &
             is.na(`East WAT1 Tile Flow`))) %>%
  mutate(Minutes = minute(Date)) %>%
  filter(!(Date > as_datetime("2013-02-22 08:00:00") &
             Minutes %in% c(5, 10, 20, 25, 35, 40, 50, 55)))


#CRAWF ===========
# generate complete date-time sequence 
Date <- list(Date = seq(from = ymd_hm("2008-01-01 00:00", tz = "GMT"),
                        to   = ymd_hm("2014-12-31 23:30", tz = "GMT"),
                        by   = "30 mins")) %>%
  as.tibble() %>% 
  mutate(Date = with_tz(Date, tzone = "UTC"))

new_crawf <-  tibble(Time = 0)
read_excel(path = "~/TD/Research Data/CRAWF/CRAWF Tile Flow_20170905_manually_modified.xlsx",
           sheet = "2011", n_max = 1) %>% names() %>% word(1) -> crawf_names
for (i in 2008:2014) {
  read_excel("~/TD/Research Data/CRAWF/CRAWF Tile Flow_20170905_manually_modified.xlsx", 
             sheet = as.character(i),
             skip = 2, 
             col_types = c(rep("date", 2), rep("numeric", 2)),
             col_names = crawf_names) -> temp
  new_crawf <- bind_rows(new_crawf, temp)
}

# calculate cumulative flow at 30 min intervals 
new_crawf %>%
  filter(!is.na(Date)) %>%
  mutate(Time = as_datetime(Time)) %>%
  mutate(Date = ymd_hm(paste(Date, format(Time, "%H:%M"))),
         Date = as_datetime(Date)) %>%
  mutate(Time = as_datetime(floor(as.numeric(Date)/1800)*1800)) %>% 
  gather(key = plotid, value = flow, 3:4) %>%
  filter(!is.na(flow)) %>%
  group_by(Time, plotid) %>%
  summarise(flow = sum(flow),
            Date = first(Date)) %>%
  arrange(plotid, Date) %>%
  ungroup() %>%
  select(Date, plotid, flow) %>%
  spread(plotid, flow) %>%
# join flow with DATE data
  full_join(Date, by = "Date") %>%
  arrange(Date) %>%
  gather(key = plotid, value = flow, 2:3) %>%
  ungroup() -> my_new_crawf

# finding gaps and saving as CSV
my_new_crawf %>%
  gapfinder() %>% 
  mutate(site = "CRAWF") %>%
  select(site, everything(), - time_gap) %>%
  mutate(start_time = as.Date(start_time),
         end_time   = as.Date(end_time),
         duration = str_replace(duration, " 0S", "")) %>%
  ungroup() %>%
  write.xlsx(paste0(newdir, "CRAWF_gaps.xlsx"), sheetName = "flow")


# replot missing data gaps
my_new_crawf %>%
  gapplot() +
  ggtitle("CRAWF", subtitle = "Tile Flow Data") +
  ggsave(file = paste0(newdir, "CRAWF_gap.png"), width = 8, height = 4)


# generate complete data 
my_new_crawf %>%
  mutate(year = year(Date),
         date = as_date(Date),
         time = format(Date, "%H:%M")) %>%
  spread(key = plotid, value = flow) %>%
  select(year, date, time, everything(), -Date) %>% 
  write_csv("~/TD/Research Data/_Missing_Data/Flow/new_spreadsheet/new_CRAWF.csv",
            na = '')



#DPAC ===========
# read file with filled data by Samaneh
dpac <- data.frame(year = 0)
for (i in 2006:2016) {
  read_excel("~/TD/Research Data/DPAC/FILL - DPAC Tile Flow_20170901.xlsx", 
             sheet = as.character(i),
             col_names = FALSE, skip = 2, col_types = c(rep("date", 2), rep("numeric", 4))) -> temp
  dpac <- bind_rows(dpac, temp)
}
names(dpac)[-1] <- read_excel("~/TD/Research Data/DPAC/FILL - DPAC Tile Flow_20170901.xlsx", 
                         n_max = 1) %>% 
  names() %>% word(1)

# generate complete date-time sequence 
Date <- list(Date = seq(from = ymd("2006-01-01"),
                        to   = ymd("2016-12-31"),
                        by   = "day")) %>%
  as.tibble() %>%
  mutate(Date = as_datetime(Date))


# join flow with DATE data
dpac %>%
  as.tibble() %>%
  select(-year, -Time) %>%
  filter(!is.na(Date)) %>%
  filter(!duplicated(.)) %>%
  full_join(Date, by = "Date") %>%
  arrange(Date) %>%
  gather(key = plotid, value = flow, 2:5) %>%
  ungroup() -> new_dpac

# finding gaps and saving as CSV
new_dpac %>%
  gapfinder() %>% 
  mutate(site = "DPAC") %>%
  select(site, everything(), - time_gap) %>%
  mutate(start_time = as.Date(start_time),
         end_time   = as.Date(end_time),
         duration = word(duration, 1)) %>%
  ungroup() %>%
  write.xlsx(paste0(newdir, "DPAC_gaps.xlsx"), sheetName = "flow")


# replot missing data gaps
new_dpac %>%
  gapplot() +
  ggtitle("DPAC", subtitle = "Tile Flow Data") +
  ggsave(file = paste0(newdir, "DPAC_gap.png"), width = 8, height = 4)


# generate complete data 
new_dpac %>%
  mutate(year = year(Date),
         date = as_date(Date),
         time = format(Date, "%H:%M")) %>%
  spread(key = plotid, value = flow) %>%
  select(year, date, time, everything(), -Date) %>% 
  write_csv("~/TD/Research Data/_Missing_Data/Flow/new_spreadsheet/new_DPAC.csv",
            na = '')

#DEFI_M ===========
# generate complete date-time sequence 
Date <- list(Date = seq(from = ymd("2008-01-01"),
                        to   = ymd("2014-12-31"),
                        by   = "day")) %>%
  as.tibble() %>%
  mutate(Date = as_datetime(Date))

# join flow with DATE data
defi_m %>% 
  filter(!duplicated(.)) %>%
  select(-comment) %>%
  mutate(Date = as.Date(Date)) %>% 
  group_by(Date, plotid) %>%
  summarise(flow = sum(flow)) %>%
  spread(key = plotid, value = flow) %>%
  ungroup() %>%
  mutate(Date = as_datetime(Date)) %>%
  full_join(Date, by = "Date") %>%
  arrange(Date) %>%
  gather(key = plotid, value = flow, 2:3) %>%
  ungroup() -> new_defi_m

# finding gaps and saving as CSV
new_defi_m %>%
  gapfinder() %>% 
  mutate(site = "DEFI_M") %>%
  select(site, everything(), - time_gap) %>%
  mutate(start_time = as.Date(start_time),
         end_time   = as.Date(end_time),
         duration = word(duration, 1)) %>%
  ungroup() %>%
  write.xlsx(paste0(newdir, "DEFI_M_gaps.xlsx"), sheetName = "flow")


# replot missing data gaps
new_defi_m %>%
  gapplot() +
  ggtitle("DEFI_M", subtitle = "Tile Flow Data") +
  ggsave(file = paste0(newdir, "DEFI_M_gap.png"), width = 8, height = 4)


# generate complete data 
new_defi_m %>%
  mutate(year = year(Date),
         date = as_date(Date),
         time = format(Date, "%H:%M")) %>%
  spread(key = plotid, value = flow) %>%
  select(year, date, time, everything(), -Date) %>% 
  write_csv("~/TD/Research Data/_Missing_Data/Flow/new_spreadsheet/new_DEFI_M.csv",
            na = '')


#FAIRM ===========
# generate complete date-time sequence 
Date <- list(Date = seq(from = ymd("2013-01-01"),
                        to   = ymd("2016-12-31"),
                        by   = "day")) %>%
  as.tibble() %>%
  mutate(Date = as_datetime(Date))


# join flow with DATE data
fairmont %>% 
  filter(!duplicated(.)) %>%
  select(-comment) %>%
  spread(key = plotid, value = flow) %>%
  full_join(Date, by = "Date") %>%
  arrange(Date) %>%
  gather(key = plotid, value = flow, 2:3) %>%
  ungroup() -> new_fairmont

# finding gaps and saving as CSV
new_fairmont %>%
  gapfinder() %>% 
  mutate(site = "FAIRM") %>%
  select(site, everything(), - time_gap) %>%
  mutate(start_time = as.Date(start_time),
         end_time   = as.Date(end_time),
         duration = word(duration, 1)) %>%
  ungroup() %>%
  write.xlsx(paste0(newdir, "FAIRM_gaps.xlsx"), sheetName = "flow")


# replot missing data gaps
new_fairmont %>%
  gapplot() +
  ggtitle("FAIRM", subtitle = "Tile Flow Data") +
  ggsave(file = paste0(newdir, "FAIRM_gap.png"), width = 8, height = 4)


# generate complete data 
new_fairmont %>%
  mutate(year = year(Date),
         date = as_date(Date),
         time = format(Date, "%H:%M")) %>%
  spread(key = plotid, value = flow) %>%
  select(year, date, time, everything(), -Date) %>% 
  write_csv("~/TD/Research Data/_Missing_Data/Flow/new_spreadsheet/new_FAIRM.csv",
            na = '')



#HARDIN ===========
# generate complete date-time sequence 
Date <- list(Date = seq(from = ymd("2008-01-01"),
                        to   = ymd("2014-12-31"),
                        by   = "day")) %>%
  as.tibble() %>%
  mutate(Date = as_datetime(Date))

# join flow with DATE data
hardin %>% 
  filter(!duplicated(.)) %>%
  select(-comment) %>%
  mutate(Date = as.Date(Date)) %>% 
  group_by(Date, plotid) %>%
  summarise(flow = sum(flow)) %>%
  spread(key = plotid, value = flow) %>%
  ungroup() %>%
  mutate(Date = as_datetime(Date)) %>%
  full_join(Date, by = "Date") %>%
  arrange(Date) %>%
  gather(key = plotid, value = flow, 2:3) %>%
  ungroup() -> new_hardin

# finding gaps and saving as CSV
new_hardin %>%
  gapfinder() %>% 
  mutate(site = "HARDIN") %>%
  select(site, everything(), - time_gap) %>%
  mutate(start_time = as.Date(start_time),
         end_time   = as.Date(end_time),
         duration = word(duration, 1)) %>%
  ungroup() %>%
  write.xlsx(paste0(newdir, "HARDIN_gaps.xlsx"), sheetName = "flow")


# replot missing data gaps
new_hardin %>%
  gapplot() +
  ggtitle("HARDIN", subtitle = "Tile Flow Data") +
  ggsave(file = paste0(newdir, "HARDIN_gap.png"), width = 8, height = 4)


# generate complete data 
new_hardin %>%
  mutate(year = year(Date),
         date = as_date(Date),
         time = format(Date, "%H:%M")) %>%
  spread(key = plotid, value = flow) %>%
  select(year, date, time, everything(), -Date) %>% 
  write_csv("~/TD/Research Data/_Missing_Data/Flow/new_spreadsheet/new_HARDIN.csv",
            na = '')


#HARDIN_NW ===========
# generate complete date-time sequence 
Date <- list(Date = seq(from = ymd("2008-01-01"),
                        to   = ymd("2014-12-31"),
                        by   = "day")) %>%
  as.tibble() %>%
  mutate(Date = as_datetime(Date))

# join flow with DATE data
hardin_nw %>% 
  filter(!duplicated(.)) %>%
  select(-comment) %>%
  filter(flow >= 0) %>%
  spread(key = plotid, value = flow) %>%
  full_join(Date, by = "Date") %>%
  arrange(Date) %>%
  gather(key = plotid, value = flow, 2:3) %>%
  ungroup() -> new_hardin_nw

# finding gaps and saving as CSV
new_hardin_nw %>%
  gapfinder() %>% 
  mutate(site = "HARDIN_NW") %>%
  select(site, everything(), - time_gap) %>%
  mutate(start_time = as.Date(start_time),
         end_time   = as.Date(end_time),
         duration = word(duration, 1)) %>%
  ungroup() %>%
  write.xlsx(paste0(newdir, "HARDIN_NW_gaps.xlsx"), sheetName = "flow")


# replot missing data gaps
new_hardin_nw %>%
  gapplot() +
  ggtitle("HARDIN_NW", subtitle = "Tile Flow Data") +
  ggsave(file = paste0(newdir, "HARDIN_NW_gap.png"), width = 8, height = 4)


# generate complete data 
new_hardin_nw %>%
  mutate(year = year(Date),
         date = as_date(Date),
         time = format(Date, "%H:%M")) %>%
  spread(key = plotid, value = flow) %>%
  select(year, date, time, everything(), -Date) %>% 
  write_csv("~/TD/Research Data/_Missing_Data/Flow/new_spreadsheet/new_HARDIN_NW.csv",
            na = '')


#HENRY ===========
# generate complete date-time sequence 
Date <- list(Date = seq(from = ymd("2008-01-01"),
                        to   = ymd("2014-12-31"),
                        by   = "day")) %>%
  as.tibble() %>%
  mutate(Date = as_datetime(Date))

# join flow with DATE data
henry %>% 
  filter(!duplicated(.)) %>%
  select(-comment) %>%
  filter(flow >= 0) %>%
  mutate(Date = as.Date(Date)) %>% 
  group_by(Date, plotid) %>%
  summarise(flow = sum(flow)) %>%
  spread(key = plotid, value = flow) %>%
  ungroup() %>%
  mutate(Date = as_datetime(Date)) %>%
  full_join(Date, by = "Date") %>%
  arrange(Date) %>%
  gather(key = plotid, value = flow, 2:3) %>%
  ungroup() -> new_henry

# finding gaps and saving as CSV
new_henry %>%
  gapfinder() %>% 
  mutate(site = "HENRY") %>%
  select(site, everything(), - time_gap) %>%
  mutate(start_time = as.Date(start_time),
         end_time   = as.Date(end_time),
         duration = word(duration, 1)) %>%
  ungroup() %>%
  write.xlsx(paste0(newdir, "HENRY_gaps.xlsx"), sheetName = "flow")


# replot missing data gaps
new_henry %>%
  gapplot() +
  ggtitle("HENRY", subtitle = "Tile Flow Data") +
  ggsave(file = paste0(newdir, "HENRY_gap.png"), width = 8, height = 4)


# generate complete data 
new_henry %>%
  mutate(year = year(Date),
         date = as_date(Date),
         time = format(Date, "%H:%M")) %>%
  spread(key = plotid, value = flow) %>%
  select(year, date, time, everything(), -Date) %>% 
  write_csv("~/TD/Research Data/_Missing_Data/Flow/new_spreadsheet/new_HENRY.csv",
            na = '')


#HICKS_B ===========
# generate complete date-time sequence 
# generate daily sequence 
Date1 <- list(Date = seq(from = ymd("2006-01-01"),
                         to   = ymd("2010-12-31"),
                         by   = "day")) %>%
  as.tibble() %>% 
  mutate(Date = as_date(Date))
# generate hourly sequence 
Date2 <- list(Date = seq(from = ymd_hm("2011-01-01 00:00", tz = "GMT"),
                         to   = ymd_hm("2015-12-31 23:00", tz = "GMT"),
                         by   = "hour")) %>%
  as.tibble() %>% 
  mutate(Date = force_tz(Date, tzone = "UTC"))


# join flow with DATE data
hicks_b %>% 
  filter(year(Date) < 2011) %>%
  filter(!duplicated(.)) %>%
  mutate(Date = as_date(Date)) %>%
  select(-comment) %>%
  spread(key = plotid, value = flow) %>% 
  full_join(Date1, by = "Date") %>%
  arrange(Date) %>% 
  gather(key = plotid, value = flow, 2:3) %>%
  mutate(Date = as_datetime(Date)) %>%
  ungroup() -> new_hicks_b_1

hicks_b %>% 
  filter(year(Date) > 2010) %>%
  filter(!duplicated(.)) %>%
  select(-comment) %>%
  spread(key = plotid, value = flow) %>% 
  full_join(Date2, by = "Date") %>% 
  arrange(Date) %>% 
  gather(key = plotid, value = flow, 2:3) %>%
  ungroup() -> new_hicks_b_2

new_hicks_b <- bind_rows(new_hicks_b_1, new_hicks_b_2)


# finding gaps and saving as CSV
new_hicks_b %>%
  gapfinder() %>% 
  mutate(site = "HICKS_B") %>%
  select(site, everything(), - time_gap) %>%
  ungroup() %>%
  write.xlsx(paste0(newdir, "HICKS_B_gaps.xlsx"), sheetName = "flow")


# replot missing data gaps
new_hicks_b %>%
  gapplot() +
  ggtitle("HICKS_B", subtitle = "Tile Flow Data") +
  ggsave(file = paste0(newdir, "HICKS_B_gap.png"), width = 8, height = 4)


# generate complete data 
new_hicks_b %>%
  mutate(year = year(Date),
         date = as_date(Date),
         time = format(Date, "%H:%M")) %>%
  spread(key = plotid, value = flow) %>%
  select(year, date, time, everything(), -Date) %>% 
  write_csv("~/TD/Research Data/_Missing_Data/Flow/new_spreadsheet/new_HICKS_B.csv",
            na = '')



#MUDS2 ===========
# generate complete date-time sequence 
Date <- list(Date = seq(from = ymd("2010-01-01"),
                        to   = ymd("2013-12-31"),
                        by   = "day")) %>%
  as.tibble() %>%
  mutate(Date = as_datetime(Date))
  

# join flow with DATE data
muds2 %>% 
  filter(!duplicated(.)) %>%
  select(-comment) %>%
  spread(key = plotid, value = flow) %>%
  full_join(Date, by = "Date") %>%
  arrange(Date) %>%
  gather(key = plotid, value = flow, 2:9) %>%
  ungroup() -> new_muds2

# finding gaps and saving as CSV
new_muds2 %>%
  gapfinder() %>% 
  mutate(site = "MUDS2") %>%
  select(site, everything(), - time_gap) %>%
  mutate(start_time = as.Date(start_time),
         end_time   = as.Date(end_time),
         duration = word(duration, 1)) %>%
  ungroup() %>%
  write.xlsx(paste0(newdir, "MUDS2_gaps.xlsx"), sheetName = "flow")


# replot missing data gaps
new_muds2 %>%
  gapplot() +
  ggtitle("MUDS2", subtitle = "Tile Flow Data") +
  ggsave(file = paste0(newdir, "MUDS2_gap.png"), width = 8, height = 4)


# generate complete data 
new_muds2 %>%
  mutate(year = year(Date),
         date = as_date(Date),
         time = format(Date, "%H:%M")) %>%
  spread(key = plotid, value = flow) %>%
  select(year, date, time, everything(), -Date) %>% 
  write_csv("~/TD/Research Data/_Missing_Data/Flow/new_spreadsheet/new_MUDS2.csv",
            na = '')



#MUDS3_OLD ===========
# generate complete date-time sequence 
Date <- list(Date = seq(from = ymd("2010-01-01"),
                        to   = ymd("2013-12-31"),
                        by   = "day")) %>%
  as.tibble() %>%
  mutate(Date = as_datetime(Date))


# join flow with DATE data
muds3_old %>% 
  filter(!duplicated(.)) %>%
  select(-comment) %>%
  spread(key = plotid, value = flow) %>%
  full_join(Date, by = "Date") %>%
  arrange(Date) %>%
  gather(key = plotid, value = flow, 2:5) %>%
  ungroup() -> new_muds3_old

# finding gaps and saving as CSV
new_muds3_old %>%
  gapfinder() %>% 
  mutate(site = "MUDS3_OLD") %>%
  select(site, everything(), - time_gap) %>%
  mutate(start_time = as.Date(start_time),
         end_time   = as.Date(end_time),
         duration = word(duration, 1)) %>%
  ungroup() %>%
  write.xlsx(paste0(newdir, "MUDS3_OLD_gaps.xlsx"), sheetName = "flow")


# replot missing data gaps
new_muds3_old %>%
  gapplot() +
  ggtitle("MUDS3_OLD", subtitle = "Tile Flow Data") +
  ggsave(file = paste0(newdir, "MUDS3_OLD_gap.png"), width = 8, height = 4)


# generate complete data 
new_muds3_old %>%
  mutate(year = year(Date),
         date = as_date(Date),
         time = format(Date, "%H:%M")) %>%
  spread(key = plotid, value = flow) %>%
  select(year, date, time, everything(), -Date) %>% 
  write_csv("~/TD/Research Data/_Missing_Data/Flow/new_spreadsheet/new_MUDS3_OLD.csv",
            na = '')


#MUDS4 ===========
# generate complete date-time sequence 
Date <- list(Date = seq(from = ymd("2010-01-01"),
                        to   = ymd("2013-12-31"),
                        by   = "day")) %>%
  as.tibble() %>%
  mutate(Date = as_datetime(Date))


# join flow with DATE data
muds4 %>% 
  filter(!duplicated(.)) %>%
  select(-comment) %>%
  spread(key = plotid, value = flow) %>%
  full_join(Date, by = "Date") %>%
  arrange(Date) %>%
  gather(key = plotid, value = flow, 2:9) %>%
  ungroup() -> new_muds4

# finding gaps and saving as CSV
new_muds4 %>%
  gapfinder() %>% 
  mutate(site = "MUDS4") %>%
  select(site, everything(), - time_gap) %>%
  mutate(start_time = as.Date(start_time),
         end_time   = as.Date(end_time),
         duration = word(duration, 1)) %>%
  ungroup() %>%
  write.xlsx(paste0(newdir, "MUDS4_gaps.xlsx"), sheetName = "flow")


# replot missing data gaps
new_muds4 %>%
  gapplot() +
  ggtitle("MUDS4", subtitle = "Tile Flow Data") +
  ggsave(file = paste0(newdir, "MUDS4_gap.png"), width = 8, height = 4)


# generate complete data 
new_muds4 %>%
  mutate(year = year(Date),
         date = as_date(Date),
         time = format(Date, "%H:%M")) %>%
  spread(key = plotid, value = flow) %>%
  select(year, date, time, everything(), -Date) %>% 
  write_csv("~/TD/Research Data/_Missing_Data/Flow/new_spreadsheet/new_MUDS4.csv",
            na = '')



#SERF_SD ===========
# generate complete date-time sequence 
Date <- list(Date = c(seq(from = ymd_hm("2015-01-01 00:00", tz = Sys.timezone()),
                          to   = ymd_hm("2015-04-30 08:00", tz = Sys.timezone()),
                          by   = "60 mins"),
                      seq(from = ymd_hm("2015-04-30 08:15", tz = Sys.timezone()),
                          to   = ymd_hm("2016-12-31 23:45", tz = Sys.timezone()),
                          by   = "15 mins"))) %>%
  as.tibble() %>% 
  mutate(Date = force_tz(Date, tzone = "UTC"))

# join flow with DATE data
serf_sd %>% 
  filter(!duplicated(.)) %>%
  select(-comment) %>%
  spread(key = plotid, value = flow) %>%
  full_join(Date, by = "Date") %>%
  arrange(Date) %>%
  gather(key = plotid, value = flow, 2:3) %>%
  ungroup() -> new_serf_sd

# finding gaps and saving as CSV
new_serf_sd %>%
  gapfinder() %>% 
  mutate(site = "SERF_SD") %>%
  select(site, everything(), - time_gap) %>%
  ungroup() %>%
  write.xlsx(paste0(newdir, "SERF_SD_gaps.xlsx"), sheetName = "flow")


# replot missing data gaps
new_serf_sd %>%
  gapplot() +
  ggtitle("SERF_SD", subtitle = "Tile Flow Data") +
  ggsave(file = paste0(newdir, "SERF_SD_gap.png"), width = 8, height = 4)


# generate complete data 
new_serf_sd %>%
  mutate(year = year(Date),
         date = as_date(Date),
         time = format(Date, "%H:%M")) %>%
  select(year, date, time, everything(), -Date) %>% 
  write_csv("~/TD/Research Data/_Missing_Data/Flow/new_spreadsheet/new_SERF_SD.csv",
            na = '')





#SERF_IA ======================
# generate complete date-time sequence 
Date <- list(Date = seq(from = ymd_h(paste0(min(year(serf_ia$Date)), "-01-01 00")),
                        to   = ymd_h(paste0(max(year(serf_ia$Date)), "-12-31 23")),
                        by   = "hour")) %>%
  as.tibble()

serf_ia %>%
  select(-comment) %>%
  spread(key = plotid, value = flow) %>% 
  full_join(Date, by = "Date") %>%
  arrange(Date) %>%
  write_csv("~/TD/Research Data/_Missing_Data/Flow/new_spreadsheet/new_SERF_IA.csv",
            na = '')



#STJOHNS ===========
# generate complete date-time sequence 
Date <- list(Date = c(seq(from = ymd_hm("2009-01-01 00:00", tz = "GMT"),
                          to   = ymd_hm("2011-12-31 23:30", tz = "GMT"),
                          by   = "30 mins"),
                      seq(from = ymd_hm("2012-01-01 00:00", tz = "GMT"),
                          to   = ymd_hm("2015-12-31 23:45", tz = "GMT"),
                          by   = "15 mins"))) %>%
  as.tibble() %>% 
  mutate(Date = force_tz(Date, tzone = "UTC"))


# join flow with DATE data
stj_combined %>% 
  mutate(Year = year(Date)) %>%
  filter(plotid == "WN") -> stj_WN
stj_combined %>% 
  mutate(Year = year(Date)) %>%
  filter(plotid == "WS") -> stj_WS

stj_WS %>%
  filter(Date > ymd_hm("2011-09-13 16.00"))

  
  group_by(plotid, Year) %>% 
  mutate(Date2 = lead(Date), 
         Freq = difftime(Date2, Date, units = "mins")) %>%
  mutate(Freq = na.locf(Freq,  maxgap = 1, na.rm = FALSE, fromLast = F)) %>%
  mutate(mins = minute(Date),
         Date2 = ifelse(mins %in% c(15, 45), Date + minutes(15), Date),
         Date2 = as_datetime(Date2),
         na_ditect = is.na(flow)) %>%
  group_by(Date2, plotid) %>%
  summarise(flow = sum(flow, na.rm = TRUE),
            na_ditect = sum(na_ditect),
            check_time = mean(Freq),
            Year = mean(Year)) %>%
  mutate(flow = ifelse(na_ditect == 2, NA, flow)) %>%
  mutate(flow = ifelse(na_ditect == 1 & check_time == 30, NA, flow)) %>%
  rename(Date = Date2) %>%
  ungroup() %>%
  select(Date:flow) %>%
  mutate(comment = ifelse(is.na(flow), "Did not collect", "Collected")) %>%
  arrange(plotid, Date) -> stjohns




filter(Date > ymd_hm("201112311800"), plotid == "WS")


stjohns %>% 
  mutate(groupme = ifelse(minute(Date) %in% c(15, 45), 
                          Date + minutes(15), Date),
         groupme = as_datetime(groupme)) %>%
  arrange(Date) %>%
  filter(Date > ymd_hm("201112311800"), plotid == "WS")
  
filter(!duplicated(.)) %>%
  select(-comment) %>%
  spread(key = plotid, value = flow) %>%
  full_join(Date, by = "Date") %>%
  arrange(Date) %>%
  gather(key = plotid, value = flow, 2:3) %>%
  ungroup() -> new_stjohns



# finding gaps and saving as CSV
new_stjohns %>%
  gapfinder() %>% 
  mutate(site = "STJOHNS") %>%
  select(site, everything(), - time_gap) %>%
  ungroup() %>%
  write.xlsx(paste0(newdir, "STJOHNS_gaps.xlsx"), sheetName = "flow")


# replot missing data gaps
new_stjohns %>%
  gapplot() +
  ggtitle("STJOHNS", subtitle = "Tile Flow Data") +
  ggsave(file = paste0(newdir, "STJOHNS_gap.png"), width = 8, height = 4)


# generate complete data 
new_stjohns %>%
  mutate(year = year(Date),
         date = as_date(Date),
         time = format(Date, "%H:%M")) %>%
  select(year, date, time, everything(), -Date) %>% 
  write_csv("~/TD/Research Data/_Missing_Data/Flow/new_spreadsheet/new_STJOHNS.csv",
            na = '')


  
#STORY ================
# generate complete date-time sequence 
Date <- list(Date = seq(from = ymd("2006-01-01"),
                        to   = ymd("2009-12-31"),
                        by   = "day")) %>%
  as.tibble() %>%
  mutate(Date = as_datetime(Date))

# join flow with DATE data
story %>% 
  filter(!duplicated(.)) %>%
  select(-comment) %>%
  spread(key = plotid, value = flow) %>%
  full_join(Date, by = "Date") %>%
  arrange(Date) %>%
  gather(key = plotid, value = flow, 2:7) %>%
  ungroup() -> new_story

# finding gaps and saving as CSV
new_story %>%
  gapfinder() %>% 
  mutate(site = "STORY") %>%
  select(site, everything(), - time_gap) %>%
  ungroup() 

# replot missing data gaps
new_story %>%
  gapplot() +
  ggtitle("STORY", subtitle = "Tile Flow Data") +
  ggsave(file = paste0(newdir, "STORY_gap.png"), width = 8, height = 4)



#TIDE_OLD ================
# generate complete date-time sequence 
Date <- list(Date = seq(from = ymd("2007-01-01"),
                        to   = ymd("2011-12-31"),
                        by   = "day")) %>%
  as.tibble() %>%
  mutate(Date = as_datetime(Date))

# join flow with DATE data
tide_old %>% 
  filter(!duplicated(.)) %>%
  select(-comment) %>%
  spread(key = plotid, value = flow) %>%
  full_join(Date, by = "Date") %>%
  arrange(Date) %>%
  gather(key = plotid, value = flow, 2:5) %>%
  ungroup() -> new_tide_old

# finding gaps and saving as CSV
new_tide_old %>%
  gapfinder() %>% 
  mutate(site = "TIDE_OLD") %>%
  select(site, everything(), - time_gap) %>%
  ungroup() %>%
  write.xlsx(paste0(newdir, "TIDE_OLD_gaps.xlsx"), sheetName = "flow")


# replot missing data gaps
new_tide_old %>%
  filter(year(Date) < 2010) %>%
  gapplot() +
  ggtitle("TIDE_OLD", subtitle = "Tile Flow Data") +
  ggsave(file = paste0(newdir, "TIDE_OLD_gap.png"), width = 8, height = 4)


# generate complete data 
new_tide_old %>%
  mutate(year = year(Date),
         date = as_date(Date),
         time = format(Date, "%H:%M")) %>%
  select(year, date, time, everything(), -Date) %>% 
  write_csv("~/TD/Research Data/_Missing_Data/Flow/new_spreadsheet/new_TIDE_OLD.csv",
            na = '')

