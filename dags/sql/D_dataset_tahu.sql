ALTER TABLE
  `test-project-335210.recipe_staging_dataset.dataset_tahu` ADD COLUMN
IF NOT EXISTS Main_Ingredient STRING;
CREATE OR REPLACE TABLE
  `test-project-335210.recipe_dataset.D_dataset_tahu` AS
SELECT
  IFNULL(Main_Ingredient,
    'Tahu') AS Main_Ingredient,
  Title AS Recipe_Title,
  Ingredients,
  Steps,
  Loves,
  URL
FROM
  `test-project-335210.recipe_staging_dataset.dataset_tahu`;