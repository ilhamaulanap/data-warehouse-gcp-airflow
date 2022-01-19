ALTER TABLE
  `test-project-335210.recipe_staging_dataset.dataset_telur` ADD COLUMN
IF NOT EXISTS Main_Ingredient STRING;
CREATE OR REPLACE TABLE
  `test-project-335210.recipe_dataset.D_dataset_telur` AS
SELECT
  IFNULL(Main_Ingredient,
    'Telur') AS Main_Ingredient,
  Title AS Recipe_Title,
  Ingredients,
  Steps,
  Loves,
  URL
FROM
  `test-project-335210.recipe_staging_dataset.dataset_telur`;