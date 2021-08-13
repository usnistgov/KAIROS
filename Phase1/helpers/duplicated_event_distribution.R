## load library
library(tidyverse)
library(readxl)
library(tidyr)
library(ggplot2)
cbPalette <- c("#D55E00","#009E73", "#56B4E9", "#CC79A7", "#F0E442","#999999", "#E69F00", "#0072B2")
cbbPalette <- c("#000000", "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7")

####
## Task 1 CMU Duplicated events
file_name <- "../../../../Phase_1_evaluation/Duplicated_ke_analysis/CMU_duplicated_ev_all_distribution.xlsx"
input_data <- read_excel(file_name)

ggplot(input_data, aes(member_ev_count)) +
  geom_bar() +
  theme(plot.title = element_text(hjust = 0.5)) +
  scale_fill_manual(values=cbPalette) +
  xlab("Number of Duplicated Member Events") +
  ylab("Number of Event Groups")
plot_name <- paste0("../../../../Phase_1_evaluation/Figures/CMU_duplicated_event_all_distribution.png")
ggsave(plot_name)


## Task 1 IBM Duplicated events
file_name <- "../../../../Phase_1_evaluation/Duplicated_ke_analysis/IBM_duplicated_ev_all_distribution.xlsx"
input_data <- read_excel(file_name)

ggplot(input_data, aes(member_ev_count)) +
  geom_bar() +
  theme(plot.title = element_text(hjust = 0.5)) +
  scale_fill_manual(values=cbPalette) +
  xlab("Number of Duplicated Member Events") +
  ylab("Number of Event Groups")
plot_name <- paste0("../../../../Phase_1_evaluation/Figures/IBM_duplicated_event_all_distribution.png")
ggsave(plot_name)


## Task 1 JHU Duplicated events
file_name <- "../../../../Phase_1_evaluation/Duplicated_ke_analysis/JHU_duplicated_ev_all_distribution.xlsx"
input_data <- read_excel(file_name)

ggplot(input_data, aes(member_ev_count)) +
  geom_bar() +
  theme(plot.title = element_text(hjust = 0.5)) +
  scale_fill_manual(values=cbPalette) +
  xlab("Number of Duplicated Member Events") +
  ylab("Number of Event Groups")
plot_name <- paste0("../../../../Phase_1_evaluation/Figures/JHU_duplicated_event_all_distribution.png")
ggsave(plot_name)


## Task 1 RESIN Duplicated events
file_name <- "../../../../Phase_1_evaluation/Duplicated_ke_analysis/RESIN_duplicated_ev_all_distribution.xlsx"
input_data <- read_excel(file_name)

ggplot(input_data, aes(member_ev_count)) +
  geom_bar() +
  theme(plot.title = element_text(hjust = 0.5)) +
  scale_fill_manual(values=cbPalette) +
  xlab("Number of Duplicated Member Events") +
  ylab("Number of Event Groups")
plot_name <- paste0("../../../../Phase_1_evaluation/Figures/RESIN_duplicated_event_all_distribution.png")
ggsave(plot_name)

file_name <- "../../../../Phase_1_evaluation/Duplicated_ke_analysis/RESIN_duplicated_ev_wi_ta1ref_distribution.xlsx"
input_data <- read_excel(file_name)

ggplot(input_data, aes(member_ev_count)) +
  geom_bar() +
  theme(plot.title = element_text(hjust = 0.5)) +
  scale_fill_manual(values=cbPalette) +
  xlab("Number of Duplicated Member Events") +
  ylab("Number of Event Groups")
plot_name <- paste0("../../../../Phase_1_evaluation/Figures/RESIN_duplicated_event_with_ta1ref_distribution.png")
ggsave(plot_name)

file_name <- "../../../../Phase_1_evaluation/Duplicated_ke_analysis/RESIN_duplicated_ev_wo_ta1ref_distribution.xlsx"
input_data <- read_excel(file_name)

ggplot(input_data, aes(member_ev_count)) +
  geom_bar() +
  theme(plot.title = element_text(hjust = 0.5)) +
  scale_fill_manual(values=cbPalette) +
  xlab("Number of Duplicated Member Events") +
  ylab("Number of Event Groups")
plot_name <- paste0("../../../../Phase_1_evaluation/Figures/RESIN_duplicated_event_without_ta1ref_distribution.png")
ggsave(plot_name)