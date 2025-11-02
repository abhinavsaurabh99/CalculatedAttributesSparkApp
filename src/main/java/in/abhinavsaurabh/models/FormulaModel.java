package in.abhinavsaurabh.models;

public class FormulaModel {

    private Long categoryId;
    private String fieldName;
    private String formula;

    public FormulaModel() {}

    public FormulaModel(Long categoryId, String fieldName, String tokens) {
        this.categoryId = categoryId;
        this.fieldName = fieldName;
        this.formula = tokens;
    }

    public Long getCategoryId() { return categoryId; }
    public void setCategoryId(Long categoryId) { this.categoryId = categoryId; }

    public String getFieldName() { return fieldName; }
    public void setFieldName(String fieldName) { this.fieldName = fieldName; }

    public String getFormula() { return formula; }
    public void setFormula(String formula) { this.formula = formula; }
}
