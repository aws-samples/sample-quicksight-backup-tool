"""
Data models for QuickSight asset inventory.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any


@dataclass
class AssetInventory:
    """Inventory of QuickSight assets discovered for backup."""
    
    datasources: List[Dict[str, Any]] = field(default_factory=list)
    datasets: List[Dict[str, Any]] = field(default_factory=list)
    analyses: List[Dict[str, Any]] = field(default_factory=list)
    dashboards: List[Dict[str, Any]] = field(default_factory=list)
    
    @property
    def total_count(self) -> int:
        """Total number of assets in the inventory."""
        return (len(self.datasources) + len(self.datasets) + 
                len(self.analyses) + len(self.dashboards))
    
    def get_asset_arns(self, asset_type: str = None) -> List[str]:
        """Get ARNs for specific asset type or all assets."""
        arns = []
        
        if asset_type is None or asset_type == "datasources":
            arns.extend([ds.get("Arn", "") for ds in self.datasources])
        
        if asset_type is None or asset_type == "datasets":
            arns.extend([ds.get("Arn", "") for ds in self.datasets])
        
        if asset_type is None or asset_type == "analyses":
            arns.extend([analysis.get("Arn", "") for analysis in self.analyses])
        
        if asset_type is None or asset_type == "dashboards":
            arns.extend([dashboard.get("Arn", "") for dashboard in self.dashboards])
        
        return [arn for arn in arns if arn]