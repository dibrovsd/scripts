-- Проставить в БСО владельцем юзера (чтоб тестировать)

update docflow_documentevent1
set user_responsible_id = 28;

update docflow_document1
set inscompany_id = 11;
